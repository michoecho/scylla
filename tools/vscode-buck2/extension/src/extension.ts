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
}

interface TestRecord {
    root: vscode.TestItem;
    suite: vscode.TestItem;
    item: vscode.TestItem;
    target: string;
    case_name: string;
}

let controller: vscode.TestController;
let extensionContext: vscode.ExtensionContext;
const roots = new Map<string, vscode.TestItem>();
const records = new Map<vscode.TestItem, TestRecord>();

export function activate(context: vscode.ExtensionContext): void {
    extensionContext = context;
    controller = vscode.tests.createTestController("buck2-test", "Buck2");
    controller.refreshHandler = () => refreshAll();
    controller.createRunProfile(
        "Run Tests",
        vscode.TestRunProfileKind.Run,
        (request, cancellation) => runTests(request, cancellation),
        true,
    );
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

    try {
        for (const group of groups.values()) {
            if (cancellation.isCancellationRequested) {
                group.records.forEach(record => run.skipped(record.item));
                continue;
            }
            group.records.forEach(record => run.started(record.item));
            const folder = vscode.workspace.getWorkspaceFolder(vscode.Uri.file(group.folder));
            if (!folder) {
                throw new Error(`No workspace folder for ${group.folder}`);
            }
            const cases = group.records.map(record => record.case_name);
            const response = await withTempOutput(async output => {
                await runBuck2(folder, [group.target], output, [], cases, cancellation);
                return readOutput(output);
            });
            const results = new Map(response.results.map(result => [caseKey(result.target, result.case_name), result]));
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
    cases?: string[],
    cancellation?: vscode.CancellationToken,
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
        ...(cases ?? []).flatMap(testCase => ["--vscode-case", testCase]),
    ];
    const args = [
        "test",
        "--config", `test.v2_test_executor=${executor}`,
        "--console", "simple",
        "--no-interactive-console",
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
