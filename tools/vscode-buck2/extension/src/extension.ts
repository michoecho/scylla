import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { spawn } from "child_process";
import * as vscode from "vscode";

interface DiscoveredCase {
    target: string;
    label: string;
    case_name: string;
    uri?: string;
    line?: number;
    labels: string[];
}

interface CaseResult {
    target: string;
    case_name: string;
    status: "passed" | "failed" | "errored";
    duration_ms?: number;
    output: string;
}

interface ExecutorOutput {
    tests: DiscoveredCase[];
    results: CaseResult[];
    coverage?: string[];
}

interface TestRecord {
    root: vscode.TestItem;
    suite: vscode.TestItem;
    item: vscode.TestItem;
    target: string;
    case_name: string;
}

interface LcovSection {
    path: string;
    lines: { hit: number; instrumented: number; details: { line: number; hit: number }[] };
    branches: { hit: number; instrumented: number; details: { line: number; hit: number; branch: string }[] };
    functions: { hit: number; instrumented: number; details: { name: string; line: number; hit: number }[] };
}

let controller: vscode.TestController;
let extensionContext: vscode.ExtensionContext;
const roots = new Map<string, vscode.TestItem>();
const records = new Map<vscode.TestItem, TestRecord>();
const coverageData = new WeakMap<vscode.FileCoverage, vscode.FileCoverageDetail[]>();

export function activate(context: vscode.ExtensionContext): void {
    extensionContext = context;
    controller = vscode.tests.createTestController("buck2-test", "Buck2");
    controller.refreshHandler = () => refreshAll();
    controller.createRunProfile(
        "Run Tests",
        vscode.TestRunProfileKind.Run,
        (request, cancellation) => runTests(request, cancellation, false),
        true,
    );
    const coverageProfile = controller.createRunProfile(
        "Run Tests with Coverage",
        vscode.TestRunProfileKind.Coverage,
        (request, cancellation) => runTests(request, cancellation, true),
        true,
    );
    coverageProfile.loadDetailedCoverage = async (_, fileCoverage) => coverageData.get(fileCoverage) ?? [];
    context.subscriptions.push(
        controller,
        vscode.commands.registerCommand("buck2Test.refresh", () => refreshAll()),
    );
    void refreshAll();
}

export function deactivate(): void {}

async function refreshAll(): Promise<void> {
    await Promise.all((vscode.workspace.workspaceFolders ?? []).map(refreshFolder));
}

async function refreshFolder(folder: vscode.WorkspaceFolder): Promise<void> {
    const rootPath = folder.uri.fsPath;
    let root = roots.get(rootPath);
    if (!root) {
        root = controller.createTestItem(`buck2-root:${rootPath}`, folder.name);
        root.description = rootPath;
        controller.items.add(root);
        roots.set(rootPath, root);
    }
    for (const [item, record] of records) {
        if (record.root === root) {
            records.delete(item);
        }
    }

    const config = vscode.workspace.getConfiguration("buck2Test", folder.uri);
    const patterns = config.get<string[]>("targetPatterns", ["//..."]);
    try {
        const response = await withTempOutput(async output => {
            await runBuck2(folder, patterns, output, ["--vscode-list-only"], undefined);
            return readOutput(output);
        });
        const suites = new Map<string, vscode.TestItem>();
        for (const test of response.tests) {
            let suite = suites.get(test.target);
            if (!suite) {
                suite = controller.createTestItem(
                    `buck2-suite:${rootPath}:${test.target}`,
                    test.label,
                );
                suite.description = test.target;
                suites.set(test.target, suite);
            }
            const item = createCaseItem(root!, suite, rootPath, test);
            suite.children.add(item);
        }
        root.children.replace([...suites.values()]);
    } catch (error) {
        root.children.replace([]);
        void vscode.window.showErrorMessage(`Buck2 test discovery failed: ${errorMessage(error)}`);
    }
}

function createCaseItem(
    root: vscode.TestItem,
    suite: vscode.TestItem,
    rootPath: string,
    test: DiscoveredCase,
): vscode.TestItem {
    const item = controller.createTestItem(
        `buck2-case:${rootPath}:${test.target}:${test.case_name}`,
        test.case_name,
        test.uri ? vscode.Uri.file(resolveTestPath(rootPath, test.uri)) : undefined,
    );
    item.description = test.target;
    if (test.line !== undefined) {
        item.range = new vscode.Range(
            new vscode.Position(Math.max(0, test.line - 1), 0),
            new vscode.Position(Math.max(0, test.line - 1), 0),
        );
    }
    item.tags = test.labels.map(label => new vscode.TestTag(label));
    records.set(item, { root, suite, item, target: test.target, case_name: test.case_name });
    return item;
}

async function runTests(
    request: vscode.TestRunRequest,
    cancellation: vscode.CancellationToken,
    withCoverage: boolean,
): Promise<void> {
    const run = controller.createTestRun(request);
    const selected = [...records.values()].filter(record => {
        const included = !request.include || request.include.some(item => contains(item, record.item));
        const excluded = request.exclude?.some(item => contains(item, record.item)) ?? false;
        return included && !excluded;
    });
    const groups = new Map<string, { folder: string; target: string; records: TestRecord[] }>();
    for (const record of selected) {
        run.enqueued(record.item);
        const folder = record.root.description ?? "";
        const key = `${folder}\0${record.target}`;
        const group = groups.get(key) ?? { folder, target: record.target, records: [] };
        group.records.push(record);
        groups.set(key, group);
    }

    // Buck delivers one ExternalRunnerSpec per target. Keep the TestItem
    // bookkeeping grouped by target, but send all targets in one Buck2
    // invocation so the executor can merge every raw profile produced by this
    // VS Code run into one report.
    const projects = new Map<string, { folder: string; groups: typeof groups }>();
    for (const group of groups.values()) {
        let project = projects.get(group.folder);
        if (!project) {
            project = { folder: group.folder, groups: new Map() };
            projects.set(group.folder, project);
        }
        project.groups.set(`${group.folder}\0${group.target}`, group);
    }

    try {
        for (const project of projects.values()) {
            if (cancellation.isCancellationRequested) {
                for (const group of project.groups.values()) {
                    group.records.forEach(record => run.skipped(record.item));
                }
                continue;
            }
            for (const group of project.groups.values()) {
                group.records.forEach(record => run.started(record.item));
            }
            const folder = vscode.workspace.getWorkspaceFolder(vscode.Uri.file(project.folder));
            if (!folder) {
                throw new Error(`No workspace folder for ${project.folder}`);
            }
            const targetGroups = [...project.groups.values()];
            const targets = targetGroups.map(group => group.target);
            const cases = targetGroups.flatMap(group => group.records.map(record => ({
                target: group.target,
                case_name: record.case_name,
            })));
            const response = await withTempOutput(async output => {
                await runBuck2(
                    folder,
                    targets,
                    output,
                    withCoverage ? ["--vscode-coverage"] : [],
                    cases,
                    cancellation,
                    withCoverage ? "root//:coverage" : undefined,
                );
                const response = await readOutput(output);
                if (withCoverage) {
                    await addCoverageFiles(run, response.coverage ?? []);
                }
                return response;
            });
            const results = new Map(response.results.map(result => [caseKey(result.target, result.case_name), result]));
            for (const group of targetGroups) {
                for (const record of group.records) {
                    const result = results.get(caseKey(record.target, record.case_name));
                    if (!result) {
                        run.errored(record.item, new vscode.TestMessage("Buck2 returned no result for this test case."));
                    } else {
                        const output = normalizeCrlf(result.output);
                        if (output) {
                            run.appendOutput(output, locationFor(record.item), record.item);
                        }
                        if (result.status === "passed") {
                            run.passed(record.item, result.duration_ms);
                        } else if (result.status === "failed") {
                            run.failed(record.item, new vscode.TestMessage(output || "Buck2 test case failed."), result.duration_ms);
                        } else {
                            run.errored(record.item, new vscode.TestMessage(output || "Buck2 test case errored."));
                        }
                    }
                }
            }
        }
    } catch (error) {
        const message = new vscode.TestMessage(errorMessage(error));
        selected.forEach(record => {
            if (cancellation.isCancellationRequested) {
                run.skipped(record.item);
            } else {
                run.errored(record.item, message);
            }
        });
    } finally {
        run.end();
    }
}

async function addCoverageFiles(run: vscode.TestRun, files: string[]): Promise<void> {
    for (const file of files) {
        let contents: Uint8Array;
        try {
            contents = await fs.promises.readFile(file);
        } catch (error) {
            throw new Error(`Could not open coverage file ${file}: ${errorMessage(error)}`);
        }
        const sections = parseLcov(contents);
        for (const section of sections) {
            const coverage = new vscode.FileCoverage(
                vscode.Uri.file(section.path.trim()),
                new vscode.TestCoverageCount(section.lines.hit, section.lines.instrumented),
                new vscode.TestCoverageCount(section.branches.hit, section.branches.instrumented),
                new vscode.TestCoverageCount(section.functions.hit, section.functions.instrumented),
            );
            const lineBranches = new Map<number, vscode.BranchCoverage[]>();
            for (const branch of section.branches.details) {
                const item = new vscode.BranchCoverage(
                    branch.hit,
                    new vscode.Position(branch.line - 1, 0),
                    branch.branch,
                );
                lineBranches.set(branch.line, [...(lineBranches.get(branch.line) ?? []), item]);
            }
            const details: vscode.FileCoverageDetail[] = [];
            for (const line of section.lines.details) {
                details.push(new vscode.StatementCoverage(
                    line.hit,
                    new vscode.Position(line.line - 1, 0),
                    lineBranches.get(line.line) ?? [],
                ));
            }
            for (const declaration of section.functions.details) {
                details.push(new vscode.DeclarationCoverage(
                    declaration.name,
                    declaration.hit,
                    new vscode.Position(declaration.line - 1, 0),
                ));
            }
            coverageData.set(coverage, details);
            run.addCoverage(coverage);
        }
    }
}

function parseLcov(contents: Uint8Array): LcovSection[] {
    const sections: LcovSection[] = [];
    let section: LcovSection | undefined;
    const functions = new Map<string, { name: string; line: number; hit: number }>();
    const functionHits = new Map<string, number>();
    for (const record of Buffer.from(contents).toString("utf8").split(/\r?\n/)) {
        if (record === "TN:" || (record.startsWith("SF:") && !section)) {
            section = {
                path: "",
                lines: { hit: 0, instrumented: 0, details: [] },
                branches: { hit: 0, instrumented: 0, details: [] },
                functions: { hit: 0, instrumented: 0, details: [] },
            };
            functions.clear();
            functionHits.clear();
        }
        if (section) {
            if (record === "end_of_record") {
                for (const [name, declaration] of functions) {
                    declaration.hit = functionHits.get(name) ?? 0;
                    section.functions.details.push(declaration);
                }
                sections.push(section);
                section = undefined;
                continue;
            }
            const separator = record.indexOf(":");
            const key = separator < 0 ? "" : record.slice(0, separator);
            const value = separator < 0 ? "" : record.slice(separator + 1);
            if (key === "SF") {
                section.path = value;
            } else if (key === "FN") {
                const comma = value.indexOf(",");
                if (comma >= 0) {
                    const line = Number(value.slice(0, comma));
                    const name = value.slice(comma + 1);
                    functions.set(name, { name, line, hit: 0 });
                }
            } else if (key === "FNDA") {
                const comma = value.indexOf(",");
                if (comma >= 0) {
                    functionHits.set(value.slice(comma + 1), Number(value.slice(0, comma)) || 0);
                }
            } else if (key === "FNF") {
                section.functions.instrumented = Number(value) || 0;
            } else if (key === "FNH") {
                section.functions.hit = Number(value) || 0;
            } else if (key === "BRDA") {
                const fields = value.split(",");
                if (fields.length >= 4) {
                    section.branches.details.push({
                        line: Number(fields[0]) || 0,
                        hit: fields[3] === "-" ? 0 : Number(fields[3]) || 0,
                        branch: fields[2],
                    });
                }
            } else if (key === "BRF") {
                section.branches.instrumented = Number(value) || 0;
            } else if (key === "BRH") {
                section.branches.hit = Number(value) || 0;
            } else if (key === "DA") {
                const fields = value.split(",");
                if (fields.length >= 2) {
                    section.lines.details.push({ line: Number(fields[0]) || 0, hit: Number(fields[1]) || 0 });
                }
            } else if (key === "LF") {
                section.lines.instrumented = Number(value) || 0;
            } else if (key === "LH") {
                section.lines.hit = Number(value) || 0;
            }
        }
    }
    return sections;
}

function locationFor(item: vscode.TestItem): vscode.Location | undefined {
    return item.uri && item.range ? new vscode.Location(item.uri, item.range) : undefined;
}

function resolveTestPath(rootPath: string, testPath: string): string {
    return path.isAbsolute(testPath) ? testPath : path.join(rootPath, testPath);
}

function normalizeCrlf(output: string): string {
    return output.replace(/\r\n?/g, "\n").replace(/\n/g, "\r\n");
}

function caseKey(target: string, caseName: string): string {
    return `${target}\0${caseName}`;
}

function contains(parent: vscode.TestItem, candidate: vscode.TestItem): boolean {
    if (parent === candidate) {
        return true;
    }
    let current = candidate.parent;
    while (current) {
        if (current === parent) {
            return true;
        }
        current = current.parent;
    }
    return false;
}

async function withTempOutput<T>(action: (output: string) => Promise<T>): Promise<T> {
    const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "buck2-vscode-"));
    const output = path.join(directory, "tests.json");
    try {
        return await action(output);
    } finally {
        await fs.promises.rm(directory, { recursive: true, force: true });
    }
}

async function readOutput(output: string): Promise<ExecutorOutput> {
    const content = await fs.promises.readFile(output, "utf8");
    return JSON.parse(content) as ExecutorOutput;
}

async function runBuck2(
    folder: vscode.WorkspaceFolder,
    targets: string[],
    output: string,
    modeArgs: string[],
    cases?: { target: string; case_name: string }[],
    cancellation?: vscode.CancellationToken,
    modifier?: string,
): Promise<void> {
    const config = vscode.workspace.getConfiguration("buck2Test", folder.uri);
    const buck2 = config.get<string>("buck2Path", "buck2");
    const executor = executorPath(config.get<string>("executorPath", ""));
    const listArg = config.get<string>("listArgument", "--list-test-cases");
    const locationArg = config.get<string>("locationArgument", "--reporters=test-locations");
    const caseArg = config.get<string>("caseArgument", "--test-case={}");
    const runnerArgs = [
        "--vscode-output", output,
        "--vscode-list-arg", listArg,
        "--vscode-location-arg", locationArg,
        "--vscode-case-arg", caseArg,
        ...modeArgs,
    ];
    if (cases) {
        const selectionFile = path.join(path.dirname(output), "selection.json");
        await fs.promises.writeFile(selectionFile, JSON.stringify(cases), "utf8");
        runnerArgs.push("--vscode-selection-file", selectionFile);
    }
    const args = [
        "test",
        "--config", `test.v2_test_executor=${executor}`,
        "--console", "simple",
        "--no-interactive-console",
        ...(modifier ? ["--modifier", modifier] : []),
        ...targets,
        "--",
        ...runnerArgs,
    ];

    await new Promise<void>((resolve, reject) => {
        const child = spawn(buck2, args, { cwd: folder.uri.fsPath });
        let stderr = "";
        let cancelled = false;
        const subscription = cancellation?.onCancellationRequested(() => {
            cancelled = true;
            child.kill();
        });
        child.stderr.on("data", data => { stderr += data.toString(); });
        child.on("error", error => {
            subscription?.dispose();
            reject(error);
        });
        child.on("close", code => {
            subscription?.dispose();
            if (cancelled) {
                reject(new Error("Buck2 test run was cancelled."));
            } else if (code !== 0 && !fs.existsSync(output)) {
                reject(new Error(stderr.trim() || `buck2 test exited with code ${code}`));
            } else {
                resolve();
            }
        });
    });
}

function executorPath(configured: string): string {
    if (configured) {
        return configured;
    }
    const executable = process.platform === "win32" ? "buck2-test-executor.exe" : "buck2-test-executor";
    const bundled = path.join(extensionContext.extensionPath, "bin", executable);
    if (fs.existsSync(bundled)) {
        return bundled;
    }
    const checkout = path.join(extensionContext.extensionPath, "..", "target", "release", executable);
    if (fs.existsSync(checkout)) {
        return checkout;
    }
    throw new Error(`Buck2 executor not found at ${bundled}; build it or set buck2Test.executorPath.`);
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}
