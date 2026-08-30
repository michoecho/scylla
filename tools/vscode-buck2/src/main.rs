use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io,
    path::PathBuf,
    pin::Pin,
    process::{self, Command, Stdio},
    task::{Context, Poll},
};

use anyhow::{anyhow, Context as _};
use clap::Parser;
use prost_types::Duration as ProtoDuration;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpStream, UnixStream},
    sync::mpsc,
};
use tokio_stream::once;
use tonic::{transport::{Channel, Endpoint, Server}, Request, Response, Status};
use tower::service_fn;

mod proto {
    tonic::include_proto!("buck.test");
}

use proto::{
    execute_response2, execution_status, execution_stream, external_runner_spec_value,
    test_executor_server, test_orchestrator_client, test_stage,
    ArgValue, ArgValueContent, ConfiguredTargetHandle, Empty, ExecuteRequest2, ExternalRunnerSpec,
    ExternalRunnerSpecValue, HostSharingRequirements, TestResult, TestStage, Testing,
};

#[derive(Debug, Parser)]
#[command(name = "buck2-vscode-test-executor", trailing_var_arg = true)]
struct Cli {
    /// Buck2 supplies these context arguments before the connection arguments.
    #[arg(long, hide = true)]
    buck_trace_id: Option<String>,
    #[arg(long = "config-entry", hide = true)]
    config_entries: Vec<String>,
    /// Unix file descriptor for the Buck -> executor RPC channel.
    #[arg(long)]
    executor_fd: Option<i32>,
    /// Unix file descriptor for the executor -> Buck RPC channel.
    #[arg(long)]
    orchestrator_fd: Option<i32>,
    /// TCP address for the Buck -> executor RPC channel.
    #[arg(long)]
    executor_addr: Option<String>,
    /// TCP address for the executor -> Buck RPC channel.
    #[arg(long)]
    orchestrator_addr: Option<String>,
    /// Arguments supplied after Buck2's `--` separator.
    #[arg(last = true)]
    runner_args: Vec<String>,
}

#[derive(Debug, Default)]
struct Options {
    output: Option<PathBuf>,
    list_only: bool,
    coverage: bool,
    debug: bool,
    selected_cases: HashSet<String>,
    selection_file: Option<PathBuf>,
    list_arg: String,
    location_arg: Option<String>,
    case_arg: String,
}

#[derive(Debug, Deserialize)]
struct SelectedCase {
    target: String,
    case_name: String,
}

#[derive(Debug, Clone)]
struct MachineCaseResult {
    status: &'static str,
    duration_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct OutputFile {
    tests: Vec<DiscoveredCase>,
    results: Vec<CaseResult>,
    coverage: Vec<String>,
    debug: Vec<DebugCommand>,
}

/// A resolved, ready-to-launch invocation for one selected test case. The
/// extension turns this into a debug adapter configuration; the executor never
/// runs it itself.
#[derive(Debug, Serialize)]
struct DebugCommand {
    target: String,
    case_name: String,
    program: String,
    args: Vec<String>,
    cwd: String,
    env: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Clone)]
struct DiscoveredCase {
    target: String,
    label: String,
    case_name: String,
    uri: Option<String>,
    line: Option<u32>,
    labels: Vec<String>,
}

#[derive(Debug, Clone)]
struct ListedCase {
    name: String,
    uri: Option<String>,
    line: Option<u32>,
}

#[derive(Debug, Serialize)]
struct CaseResult {
    target: String,
    case_name: String,
    status: &'static str,
    duration_ms: Option<u64>,
    output: String,
}

#[derive(Debug)]
struct CoverageInput {
    profile: PathBuf,
    binary: PathBuf,
    workspace_root: PathBuf,
}

#[derive(Debug)]
struct PreparedCoveragePaths {
    profile: PathBuf,
    binary: PathBuf,
    workspace_root: PathBuf,
}

#[derive(Clone)]
struct ExecutorService {
    specs: mpsc::UnboundedSender<Option<ExternalRunnerSpec>>,
}

#[tonic::async_trait]
impl test_executor_server::TestExecutor for ExecutorService {
    async fn external_runner_spec(
        &self,
        request: Request<proto::ExternalRunnerSpecRequest>,
    ) -> Result<Response<Empty>, Status> {
        let spec = request
            .into_inner()
            .test_spec
            .ok_or_else(|| Status::invalid_argument("missing test_spec"))?;
        self.specs
            .send(Some(spec))
            .map_err(|_| Status::cancelled("executor stopped"))?;
        Ok(Response::new(Empty {}))
    }

    async fn end_of_test_requests(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.specs
            .send(None)
            .map_err(|_| Status::cancelled("executor stopped"))?;
        Ok(Response::new(Empty {}))
    }

    async fn unstable_heap_dump(
        &self,
        _: Request<proto::UnstableHeapDumpRequest>,
    ) -> Result<Response<proto::UnstableHeapDumpResponse>, Status> {
        Err(Status::unimplemented("heap dumps are not supported"))
    }
}

struct ServerIo<T>(T);

impl<T: AsyncRead + Unpin> AsyncRead for ServerIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ServerIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<T> tonic::transport::server::Connected for ServerIo<T> {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let options = parse_options(&cli.runner_args)?;
    let (executor_io, orchestrator_io) = connect_channels(&cli).await?;
    let (spec_sender, mut spec_receiver) = mpsc::unbounded_channel();

    let executor_server = tokio::spawn(async move {
        Server::builder()
            .add_service(test_executor_server::TestExecutorServer::new(ExecutorService {
                specs: spec_sender,
            }))
            .serve_with_incoming(once(Ok::<_, io::Error>(ServerIo(executor_io))))
            .await
            .map_err(|error| anyhow!(error))
    });

    let channel = client_channel(orchestrator_io).await?;
    let mut orchestrator = test_orchestrator_client::TestOrchestratorClient::new(channel);
    let mut output = OutputFile { tests: Vec::new(), results: Vec::new(), coverage: Vec::new(), debug: Vec::new() };
    let mut coverage_inputs = Vec::new();
    let mut exit_code = 0;

    while let Some(message) = spec_receiver.recv().await {
        let Some(spec) = message else { break };
        match process_spec(&mut orchestrator, &options, spec, &mut output, &mut coverage_inputs).await {
            Ok(()) => {}
            Err(error) => {
                eprintln!("buck2-vscode-test-executor: {error:#}");
                exit_code = 32;
            }
        }
    }

    if options.coverage && !coverage_inputs.is_empty() {
        let coverage_dir = options
            .output
            .as_ref()
            .and_then(|path| path.parent())
            .unwrap_or_else(|| std::path::Path::new("."));
        let lcov = process_coverage(coverage_dir, &coverage_inputs)?;
        output.coverage.push(lcov.to_string_lossy().into_owned());
    }

    if let Some(path) = &options.output {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory {}", parent.display()))?;
        }
        fs::write(path, serde_json::to_vec_pretty(&output)?)
            .with_context(|| format!("writing executor output {}", path.display()))?;
    }

    orchestrator
        .end_of_test_results(proto::EndOfTestResultsRequest { exit_code })
        .await
        .context("reporting end of test results")?;
    executor_server.await??;
    process::exit(0);
}

fn parse_options(args: &[String]) -> anyhow::Result<Options> {
    let mut options = Options {
        list_arg: "--list-test-cases".to_owned(),
        case_arg: "--test-case={}".to_owned(),
        location_arg: None,
        ..Default::default()
    };
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        let (key, value) = arg.split_once('=').unwrap_or((arg, ""));
        match key {
            "--vscode-output" => options.output = Some(PathBuf::from(value_or_next(args, index, value)?)),
            "--vscode-list-only" => options.list_only = true,
            "--vscode-coverage" => options.coverage = true,
            "--vscode-debug" => options.debug = true,
            "--vscode-case" => {
                options.selected_cases.insert(value_or_next(args, index, value)?.to_owned());
            }
            "--vscode-selection-file" => {
                options.selection_file = Some(PathBuf::from(value_or_next(args, index, value)?));
            }
            "--vscode-list-arg" => options.list_arg = value_or_next(args, index, value)?.to_owned(),
            "--vscode-location-arg" => {
                let location_arg = value_or_next(args, index, value)?;
                options.location_arg = (!location_arg.is_empty()).then(|| location_arg.to_owned());
            }
            "--vscode-case-arg" => options.case_arg = value_or_next(args, index, value)?.to_owned(),
            _ => {}
        }
        if value.is_empty() && matches!(key, "--vscode-output" | "--vscode-case" | "--vscode-selection-file" | "--vscode-list-arg" | "--vscode-location-arg" | "--vscode-case-arg") {
            index += 1;
        }
        index += 1;
    }
    if let Some(path) = &options.selection_file {
        let selected = serde_json::from_slice::<Vec<SelectedCase>>(
            &fs::read(path).with_context(|| format!("reading VS Code selection file {}", path.display()))?,
        ).with_context(|| format!("parsing VS Code selection file {}", path.display()))?;
        for case in selected {
            options.selected_cases.insert(format!("{}\u{1f}{}", case.target, case.case_name));
        }
    }
    Ok(options)
}

fn value_or_next<'a>(args: &'a [String], index: usize, value: &'a str) -> anyhow::Result<&'a str> {
    if !value.is_empty() {
        return Ok(value);
    }
    args.get(index + 1)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("missing value for {}", args[index]))
}

async fn connect_channels(cli: &Cli) -> anyhow::Result<(BoxIo, BoxIo)> {
    if let (Some(executor_fd), Some(orchestrator_fd)) = (cli.executor_fd, cli.orchestrator_fd) {
        #[cfg(unix)]
        {
            use std::os::fd::FromRawFd;
            let executor = unsafe { UnixStream::from_std(std::os::unix::net::UnixStream::from_raw_fd(executor_fd))? };
            let orchestrator = unsafe { UnixStream::from_std(std::os::unix::net::UnixStream::from_raw_fd(orchestrator_fd))? };
            return Ok((BoxIo::Unix(executor), BoxIo::Unix(orchestrator)));
        }
        #[cfg(not(unix))]
        {
            let _ = (executor_fd, orchestrator_fd);
            return Err(anyhow!("file descriptor transport is unavailable on this platform"));
        }
    }

    let executor_addr = cli.executor_addr.as_deref().ok_or_else(|| anyhow!("missing --executor-addr"))?;
    let orchestrator_addr = cli.orchestrator_addr.as_deref().ok_or_else(|| anyhow!("missing --orchestrator-addr"))?;
    Ok((
        BoxIo::Tcp(TcpStream::connect(executor_addr).await?),
        BoxIo::Tcp(TcpStream::connect(orchestrator_addr).await?),
    ))
}

enum BoxIo {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl AsyncRead for BoxIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)] Self::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BoxIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)] Self::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)] Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)] Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

async fn client_channel(io: BoxIo) -> anyhow::Result<Channel> {
    let mut io = Some(io);
    Ok(Endpoint::from_static("http://buck2.test.executor")
        .connect_with_connector(service_fn(move |_| {
            let io = io.take().ok_or_else(|| io::Error::other("channel reused"));
            async move { io.map(hyper_util::rt::TokioIo::new) }
        }))
        .await?)
}

async fn process_spec(
    orchestrator: &mut test_orchestrator_client::TestOrchestratorClient<Channel>,
    options: &Options,
    spec: ExternalRunnerSpec,
    output: &mut OutputFile,
    coverage_inputs: &mut Vec<CoverageInput>,
) -> anyhow::Result<()> {
    let target = spec.target.clone().ok_or_else(|| anyhow!("test spec has no target"))?;
    let handle = target.handle.clone().ok_or_else(|| anyhow!("test spec has no target handle"))?;
    let target_name = format!("{}//{}:{}", target.cell, target.package, target.target);
    let label = target.target.clone();
    let mut list_command = command_with_arg(&spec.command, &options.list_arg);
    if let Some(location_arg) = &options.location_arg {
        list_command.push(verbatim_arg(location_arg));
    }
    let listing = execute(
        orchestrator,
        &handle,
        &spec,
        TestStage { item: Some(test_stage::Item::Listing(test_stage::Listing { suite: target_name.clone(), cacheable: true })) },
        list_command,
        options.coverage.then(|| execution_env(&spec, Some(verbatim_arg("/dev/null")))),
    ).await?;
    let listing_output = execution_output(&listing);
    let cases = parse_listing(&execution_stream_output(listing.stdout.as_ref()))?;

    if !listing_succeeded(&listing) {
        orchestrator.report_test_result(proto::ReportTestResultRequest { result: Some(TestResult {
            name: format!("{target_name} - listing"),
            status: proto::TestStatus::ListingFailed as i32,
            msg: None,
            target: Some(handle),
            duration: listing.execution_time.clone(),
            details: listing_output.clone(),
            max_memory_used_bytes: listing.max_memory_used_bytes,
        }) }).await?;
        return Err(anyhow!("listing failed for {target_name}\n{listing_output}"));
    }

    let cases = if options.selected_cases.is_empty() {
        cases
    } else {
        cases.into_iter().filter(|case| {
            // The extension qualifies selections when one Buck invocation
            // covers several targets. Keep accepting the old unqualified
            // spelling for callers that invoke the executor directly.
            options.selected_cases.contains(&format!("{}\u{1f}{}", target_name, case.name))
                || options.selected_cases.contains(&case.name)
        }).collect()
    };
    let discovered = cases.iter().map(|case| DiscoveredCase {
        target: target_name.clone(),
        label: label.clone(),
        case_name: case.name.clone(),
        uri: case.uri.clone(),
        line: case.line,
        labels: spec.labels.clone(),
    }).collect::<Vec<_>>();
    orchestrator.report_tests_discovered( proto::ReportTestsDiscoveredRequest {
        target: Some(handle.clone()),
        testing: Some(Testing { suite: target_name.clone(), testcases: cases.iter().map(|case| case.name.clone()).collect(), variant: None, repeat_count: None }),
    }).await?;
    output.tests.extend(discovered);

    if options.list_only {
        return Ok(());
    }

    // Debugging never executes the test through Buck. It asks Buck to
    // materialise the test binary and resolve the command line it would have
    // run, then hands that to the extension so a debug adapter can launch it.
    if options.debug {
        for case in &cases {
            let case_filter = if options.case_arg == "--test-case={}" {
                escape_doctest_filter(&case.name)
            } else {
                case.name.clone()
            };
            let command = command_with_arg(&spec.command, &options.case_arg.replace("{}", &case_filter));
            let stage = TestStage {
                item: Some(test_stage::Item::Testing(Testing {
                    suite: target_name.clone(),
                    testcases: vec![case.name.clone()],
                    variant: None,
                    repeat_count: None,
                })),
            };
            let prepared = prepare_local_execution(
                orchestrator,
                &handle,
                stage,
                command,
                execution_env(&spec, None),
            ).await?;
            let mut argv = prepared.cmd.into_iter();
            let program = argv
                .next()
                .ok_or_else(|| anyhow!("Buck2 returned no test executable for {target_name}"))?;
            output.debug.push(DebugCommand {
                target: target_name.clone(),
                case_name: case.name.clone(),
                program: resolve_prepared_path(&prepared.cwd, &program)
                    .to_string_lossy()
                    .into_owned(),
                args: argv.collect(),
                env: prepared.env.into_iter().map(|entry| (entry.key, entry.value)).collect(),
                cwd: prepared.cwd,
            });
        }
        return Ok(());
    }

    if spec.labels.iter().any(|label| label == "startup_shared") && !cases.is_empty() {
        if options.case_arg != "--test-case={}" {
            return Err(anyhow!(
                "startup_shared tests require buck2Test.caseArgument to remain --test-case={{}}"
            ));
        }

        let case_names = cases.iter().map(|case| case.name.clone()).collect::<Vec<_>>();
        let case_filter = cases
            .iter()
            .map(|case| escape_doctest_filter(&case.name))
            .collect::<Vec<_>>()
            .join(",");
        let mut command = command_with_arg(&spec.command, &options.case_arg.replace("{}", &case_filter));
        command.push(verbatim_arg("--reporters=console,vscode-results"));
        let stage = TestStage {
            item: Some(test_stage::Item::Testing(Testing {
                suite: target_name.clone(),
                testcases: case_names.clone(),
                variant: None,
                repeat_count: None,
            })),
        };
        let coverage_output = coverage_output_name(&target_name, "startup-shared");
        let env = execution_env(&spec, options.coverage.then(|| declared_output(&coverage_output)));
        let coverage_paths = if options.coverage {
            Some(prepare_coverage_paths(
                orchestrator,
                &handle,
                stage.clone(),
                &command,
                declared_output(&coverage_output),
                env.clone(),
            ).await?)
        } else {
            None
        };
        let result = execute(orchestrator, &handle, &spec, stage, command, Some(env)).await?;
        if let Some(paths) = coverage_paths {
            coverage_inputs.push(CoverageInput {
                profile: paths.profile,
                binary: paths.binary,
                workspace_root: paths.workspace_root,
            });
        }

        let stdout = execution_stream_output(result.stdout.as_ref());
        let stderr = execution_stream_output(result.stderr.as_ref());
        let case_outputs = parse_case_outputs(&stdout, &stderr)?;
        let machine_results = parse_machine_results(&stdout)?;
        if machine_results.is_empty() {
            return Err(anyhow!(
                "startup_shared test produced no per-case results for {target_name}"
            ));
        }
        for case_name in case_names {
            let machine = machine_results.get(&case_name);
            let case_output = case_outputs
                .get(&case_name)
                .cloned()
                .unwrap_or_default();
            let case_details = format_case_output(&case_output);
            let (buck_status, status, duration_ms) = match machine {
                Some(result) if result.status == "passed" => (
                    proto::TestStatus::Pass as i32,
                    "passed",
                    result.duration_ms,
                ),
                Some(result) if result.status == "failed" => (
                    proto::TestStatus::Fail as i32,
                    "failed",
                    result.duration_ms,
                ),
                Some(result) => (
                    proto::TestStatus::Unknown as i32,
                    "errored",
                    result.duration_ms,
                ),
                None => (
                    proto::TestStatus::Unknown as i32,
                    "errored",
                    None,
                ),
            };
            let details = if status == "passed" {
                format_case_summary(&case_output).unwrap_or_else(|| case_details.clone())
            } else {
                case_details.clone()
            };
            orchestrator.report_test_result(proto::ReportTestResultRequest {
                result: Some(TestResult {
                    name: case_name.clone(),
                    status: buck_status,
                    msg: None,
                    target: Some(handle.clone()),
                    duration: duration_ms.map(proto_duration),
                    details,
                    max_memory_used_bytes: result.max_memory_used_bytes,
                }),
            }).await?;
            output.results.push(CaseResult {
                target: target_name.clone(),
                case_name,
                status,
                duration_ms,
                output: case_details,
            });
        }
        return Ok(());
    }

    for case in cases {
        let case_name = case.name;
        let case_filter = if options.case_arg == "--test-case={}" {
            escape_doctest_filter(&case_name)
        } else {
            case_name.clone()
        };
        let mut command = command_with_arg(&spec.command, &options.case_arg.replace("{}", &case_filter));
        command.push(verbatim_arg("--reporters=console,vscode-results"));
        let stage = TestStage { item: Some(test_stage::Item::Testing(Testing { suite: target_name.clone(), testcases: vec![case_name.clone()], variant: None, repeat_count: None })) };
        let coverage_output = coverage_output_name(&target_name, &case_name);
        let env = execution_env(&spec, options.coverage.then(|| declared_output(&coverage_output)));
        let coverage_paths = if options.coverage {
            Some(prepare_coverage_paths(
                orchestrator,
                &handle,
                stage.clone(),
                &command,
                declared_output(&coverage_output),
                env.clone(),
            ).await?)
        } else {
            None
        };
        let result = execute(
            orchestrator,
            &handle,
            &spec,
            stage,
            command,
            Some(env),
        ).await?;
        if let Some(paths) = coverage_paths {
            coverage_inputs.push(CoverageInput {
                profile: paths.profile,
                binary: paths.binary,
                workspace_root: paths.workspace_root,
            });
        }
        let status = result_status(&result);
        let raw_stdout = execution_stream_output(result.stdout.as_ref());
        let raw_stderr = execution_stream_output(result.stderr.as_ref());
        let case_output = parse_case_outputs(&raw_stdout, &raw_stderr)
            .ok()
            .and_then(|outputs| outputs.into_iter().next().map(|(_, output)| output));
        let case_details = case_output
            .as_ref()
            .map(format_case_output)
            .unwrap_or_else(|| execution_output(&result));
        let details = if status.1 == "passed" {
            case_output
                .as_ref()
                .and_then(format_case_summary)
                .unwrap_or_else(|| case_details.clone())
        } else {
            case_details.clone()
        };
        orchestrator.report_test_result(proto::ReportTestResultRequest { result: Some(TestResult {
            name: case_name.clone(),
            status: status.0,
            msg: None,
            target: Some(handle.clone()),
            duration: result.execution_time.clone(),
            details,
            max_memory_used_bytes: result.max_memory_used_bytes,
        }) }).await?;
        output.results.push(CaseResult {
            target: target_name.clone(),
            case_name,
            status: status.1,
            duration_ms: result.execution_time.as_ref().map(duration_ms),
            output: case_details,
        });
    }
    Ok(())
}

fn execution_env(spec: &ExternalRunnerSpec, profile: Option<ArgValue>) -> Vec<proto::EnvironmentVariable> {
    let mut env = spec.env.iter().map(|(key, value)| proto::EnvironmentVariable {
        key: key.clone(),
        value: Some(ArgValue {
            content: Some(ArgValueContent { value: Some(proto::arg_value_content::Value::SpecValue(value.clone())) }),
            format: None,
        }),
    }).collect::<Vec<_>>();
    if let Some(profile) = profile {
        env.push(proto::EnvironmentVariable { key: "LLVM_PROFILE_FILE".to_owned(), value: Some(profile) });
    }
    env
}

fn declared_output(name: &str) -> ArgValue {
    ArgValue {
        content: Some(ArgValueContent { value: Some(proto::arg_value_content::Value::DeclaredOutput(proto::OutputName { name: name.to_owned() })) }),
        format: None,
    }
}

async fn prepare_coverage_paths(
    orchestrator: &mut test_orchestrator_client::TestOrchestratorClient<Channel>,
    handle: &ConfiguredTargetHandle,
    stage: TestStage,
    command: &[ArgValue],
    profile: ArgValue,
    env: Vec<proto::EnvironmentVariable>,
) -> anyhow::Result<PreparedCoveragePaths> {
    let mut path_command = command.to_vec();
    path_command.push(profile);
    let prepared = prepare_local_execution(orchestrator, handle, stage, path_command, env).await?;
    let profile = prepared.cmd.last().ok_or_else(|| anyhow!("Buck2 returned an empty prepared coverage command"))?;
    let binary = prepared.cmd.first().ok_or_else(|| anyhow!("Buck2 returned no test executable"))?;
    Ok(PreparedCoveragePaths {
        profile: resolve_prepared_path(&prepared.cwd, profile),
        binary: resolve_prepared_path(&prepared.cwd, binary),
        workspace_root: PathBuf::from(prepared.cwd),
    })
}

/// Asks Buck2 to materialise the inputs for one test execution and return the
/// concrete command line, working directory and environment it would use,
/// without running anything.
async fn prepare_local_execution(
    orchestrator: &mut test_orchestrator_client::TestOrchestratorClient<Channel>,
    handle: &ConfiguredTargetHandle,
    stage: TestStage,
    cmd: Vec<ArgValue>,
    env: Vec<proto::EnvironmentVariable>,
) -> anyhow::Result<proto::PrepareForLocalExecutionResult> {
    orchestrator.prepare_for_local_execution(proto::PrepareForLocalExecutionRequest {
        test_executable: Some(proto::TestExecutable {
            stage: Some(stage),
            target: Some(handle.clone()),
            cmd,
            pre_create_dirs: Vec::new(),
            env,
        }),
        required_local_resources: Vec::new(),
    }).await?.into_inner().result.ok_or_else(|| anyhow!("Buck2 returned no prepared command"))
}

fn resolve_prepared_path(cwd: &str, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() || cwd.is_empty() {
        path
    } else {
        PathBuf::from(cwd).join(path)
    }
}

fn coverage_output_name(target: &str, case: &str) -> String {
    format!("vscode-coverage/{:016x}/{:016x}.profraw", stable_hash(target), stable_hash(case))
}

fn stable_hash(value: &str) -> u64 {
    value.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn process_coverage(
    coverage_dir: &std::path::Path,
    inputs: &[CoverageInput],
) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(coverage_dir).with_context(|| format!("creating coverage directory {}", coverage_dir.display()))?;
    let profdata = coverage_dir.join("buck2-total.profdata");
    let lcov = coverage_dir.join("buck2-total.lcov");
    let mut merge = Command::new("llvm-profdata");
    merge.arg("merge").arg("-sparse");
    for input in inputs {
        merge.arg(&input.profile);
    }
    let merge_status = merge.arg("-o").arg(&profdata).status()
        .context("starting llvm-profdata")?;
    if !merge_status.success() {
        return Err(anyhow!("llvm-profdata failed with status {merge_status}"));
    }

    let debug_dir = coverage_dir.join(".build-id");
    let mut binaries = Vec::new();
    for input in inputs {
        collect_binaries(input.binary.parent().unwrap_or_else(|| std::path::Path::new(".")), &mut binaries)?;
    }
    binaries.sort();
    binaries.dedup();

    for binary in binaries {
        let Some(build_id) = read_build_id(&binary)? else { continue };
        if build_id.len() < 3 {
            return Err(anyhow!("invalid build ID '{}' in {}", build_id, binary.display()));
        }
        let link_dir = debug_dir.join(&build_id[..2]);
        fs::create_dir_all(&link_dir)
            .with_context(|| format!("creating build-ID directory {}", link_dir.display()))?;
        let link = link_dir.join(format!("{}.debug", &build_id[2..]));
        match fs::symlink_metadata(&link) {
            Ok(_) => fs::remove_file(&link)
                .with_context(|| format!("removing stale build-ID link {}", link.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).with_context(|| format!("checking build-ID link {}", link.display())),
        }
        symlink_file(&binary, &link)
            .with_context(|| format!("linking {} as {}", binary.display(), link.display()))?;
    }

    let export = Command::new("llvm-cov")
        .arg("export")
        .arg("-format=lcov")
        .arg("--check-binary-ids")
        .arg("--debug-file-directory")
        .arg(coverage_dir)
        .arg("-instr-profile")
        .arg(&profdata)
        .stdout(Stdio::piped())
        .output()
        .context("starting llvm-cov")?;
    if !export.status.success() {
        return Err(anyhow!("llvm-cov failed: {}", String::from_utf8_lossy(&export.stderr)));
    }
    let workspace_root = inputs
        .first()
        .map(|input| input.workspace_root.as_path())
        .unwrap_or_else(|| std::path::Path::new("."));
    let lcov_contents = normalize_lcov_paths(&export.stdout, workspace_root);
    fs::write(&lcov, lcov_contents).with_context(|| format!("writing coverage report {}", lcov.display()))?;
    Ok(lcov)
}

fn normalize_lcov_paths(contents: &[u8], workspace_root: &std::path::Path) -> Vec<u8> {
    let contents = String::from_utf8_lossy(contents);
    let mut normalized = String::with_capacity(contents.len());
    for line in contents.split_inclusive('\n') {
        let (body, newline) = line.strip_suffix('\n').map_or((line, ""), |body| (body, "\n"));
        if let Some(path) = body.strip_prefix("SF:") {
            normalized.push_str("SF:");
            normalized.push_str(&normalize_source_path(path, workspace_root));
            normalized.push_str(newline);
        } else {
            normalized.push_str(line);
        }
    }
    normalized.into_bytes()
}

fn normalize_source_path(path: &str, workspace_root: &std::path::Path) -> String {
    let path = std::path::Path::new(path);
    if path.is_relative() {
        let local = workspace_root.join(path);
        return if local.is_file() {
            local.to_string_lossy().into_owned()
        } else {
            path.to_string_lossy().into_owned()
        };
    }

    if path.is_file() {
        return path.to_string_lossy().into_owned();
    }

    let components = path.components().collect::<Vec<_>>();
    for start in 1..components.len() {
        let mut relative = PathBuf::new();
        for component in &components[start..] {
            relative.push(component.as_os_str());
        }
        let local = workspace_root.join(&relative);
        if local.is_file() {
            return local.to_string_lossy().into_owned();
        }
    }
    path.to_string_lossy().into_owned()
}

fn collect_binaries(dir: &std::path::Path, binaries: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("scanning {} for coverage binaries", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_binaries(&path, binaries)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            // readelf is the authoritative ELF check here; the directory also
            // contains argsfiles and object files that should simply be ignored.
            if read_build_id(&path)?.is_some() {
                binaries.push(path);
            }
        }
    }
    Ok(())
}

fn read_build_id(binary: &std::path::Path) -> anyhow::Result<Option<String>> {
    let output = Command::new("readelf")
        .arg("-n")
        .arg(binary)
        .output()
        .with_context(|| format!("reading build ID from {}", binary.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text.lines().find_map(|line| {
        line.trim().strip_prefix("Build ID:").map(|id| id.trim().to_owned())
    }))
}

fn symlink_file(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(target, link)
    }
}

fn command_with_arg(command: &[ExternalRunnerSpecValue], arg: &str) -> Vec<ArgValue> {
    command.iter().cloned().map(|value| ArgValue {
        content: Some(ArgValueContent { value: Some(proto::arg_value_content::Value::SpecValue(value)) }),
        format: None,
    }).chain([verbatim_arg(arg)]).collect()
}

fn verbatim_arg(arg: &str) -> ArgValue {
    ArgValue {
        content: Some(ArgValueContent { value: Some(proto::arg_value_content::Value::SpecValue(ExternalRunnerSpecValue {
            value: Some(external_runner_spec_value::Value::Verbatim(arg.to_owned())),
        })) }),
        format: None,
    }
}

fn escape_doctest_filter(case_name: &str) -> String {
    let mut escaped = String::with_capacity(case_name.len());
    for character in case_name.chars() {
        if matches!(character, ',' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn parse_machine_results(output: &str) -> anyhow::Result<HashMap<String, MachineCaseResult>> {
    const PREFIX: &str = "VSCODE_TEST_RESULT\t";
    let mut results = HashMap::new();
    for line in output.lines() {
        let Some(fields) = line.strip_prefix(PREFIX) else { continue };
        let fields = fields.split('\t').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(anyhow!("malformed VS Code test result record: {line:?}"));
        }
        let case_name = decode_hex(fields[0])?;
        let status = match fields[1] {
            "passed" => "passed",
            "failed" => "failed",
            other => return Err(anyhow!("unknown VS Code test result status {other:?}")),
        };
        let duration_ms = fields[2]
            .parse::<u64>()
            .with_context(|| format!("invalid VS Code test duration {:?}", fields[2]))?;
        results.insert(case_name, MachineCaseResult { status, duration_ms: Some(duration_ms) });
    }
    Ok(results)
}

fn decode_hex(value: &str) -> anyhow::Result<String> {
    if value.len() % 2 != 0 {
        return Err(anyhow!("odd-length hexadecimal test name"));
    }
    let bytes = (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .context("invalid hexadecimal test name")?;
    String::from_utf8(bytes).context("machine-readable test name is not UTF-8")
}

fn proto_duration(milliseconds: u64) -> ProtoDuration {
    ProtoDuration {
        seconds: (milliseconds / 1000) as i64,
        nanos: ((milliseconds % 1000) * 1_000_000) as i32,
    }
}

async fn execute(
    orchestrator: &mut test_orchestrator_client::TestOrchestratorClient<Channel>,
    handle: &ConfiguredTargetHandle,
    spec: &ExternalRunnerSpec,
    stage: TestStage,
    command: Vec<ArgValue>,
    env: Option<Vec<proto::EnvironmentVariable>>,
) -> anyhow::Result<proto::ExecutionResult2> {
    let response = orchestrator.execute2(ExecuteRequest2 {
        timeout: Some(ProtoDuration { seconds: 300, nanos: 0 }),
        host_sharing_requirements: Some(HostSharingRequirements {
            requirements: Some(proto::host_sharing_requirements::Requirements::ExclusiveAccess(
                proto::host_sharing_requirements::ExclusiveAccess {},
            )),
        }),
        test_executable: Some(proto::TestExecutable {
            stage: Some(stage),
            target: Some(handle.clone()),
            cmd: command,
            pre_create_dirs: Vec::new(),
            env: env.unwrap_or_else(|| execution_env(spec, None)),
        }),
        executor_override: None,
        required_local_resources: Vec::new(),
        disable_test_execution_caching: false,
    }).await?.into_inner();
    match response.response {
        Some(execute_response2::Response::Result(result)) => Ok(result),
        Some(execute_response2::Response::Cancelled(_)) => Err(anyhow!("Buck2 cancelled execution")),
        None => Err(anyhow!("Buck2 returned no execution result")),
    }
}

fn listing_succeeded(result: &proto::ExecutionResult2) -> bool {
    matches!(result.status.as_ref().and_then(|s| s.status.as_ref()), Some(execution_status::Status::Finished(0)))
}

fn result_status(result: &proto::ExecutionResult2) -> (i32, &'static str) {
    match result.status.as_ref().and_then(|status| status.status.as_ref()) {
        Some(execution_status::Status::Finished(0)) => (proto::TestStatus::Pass as i32, "passed"),
        Some(execution_status::Status::Finished(_)) => (proto::TestStatus::Fail as i32, "failed"),
        Some(execution_status::Status::TimedOut(_)) => (proto::TestStatus::Timeout as i32, "errored"),
        None => (proto::TestStatus::Unknown as i32, "errored"),
    }
}

fn execution_output(result: &proto::ExecutionResult2) -> String {
    format_case_output(&CaseOutput {
        stdout: trim_case_output(execution_stream_output(result.stdout.as_ref())),
        stderr: trim_case_output(execution_stream_output(result.stderr.as_ref())),
    })
}

#[derive(Clone, Debug, Default)]
struct CaseOutput {
    stdout: String,
    stderr: String,
}

fn parse_case_outputs(stdout: &str, stderr: &str) -> anyhow::Result<HashMap<String, CaseOutput>> {
    let stdout = parse_case_output_stream(stdout, "stdout")?;
    let stderr = parse_case_output_stream(stderr, "stderr")?;
    let mut names = stdout.keys().chain(stderr.keys()).cloned().collect::<Vec<_>>();
    names.sort();
    names.dedup();

    Ok(names
        .into_iter()
        .map(|name| {
            let stdout_output = stdout.get(&name).map(String::as_str).unwrap_or_default();
            let stderr_output = stderr.get(&name).map(String::as_str).unwrap_or_default();
            (
                name,
                CaseOutput {
                    stdout: trim_case_output(stdout_output.to_owned()),
                    stderr: trim_case_output(stderr_output.to_owned()),
                },
            )
        })
        .collect())
}

fn trim_case_output(mut output: String) -> String {
    while output.starts_with(['\n', '\r']) {
        output.remove(0);
    }
    while output.ends_with(['\n', '\r']) {
        output.pop();
    }
    output
}

fn format_case_output(output: &CaseOutput) -> String {
    let mut sections = Vec::new();
    if !output.stdout.is_empty() {
        sections.push(format!("---- STDOUT ----\n{}", output.stdout));
    }
    if !output.stderr.is_empty() {
        sections.push(format!("---- STDERR ----\n{}", output.stderr));
    }
    sections.join("\n")
}

fn format_case_summary(output: &CaseOutput) -> Option<String> {
    let run = output.stdout.lines().find(|line| line.starts_with("RUN "))?;
    let outcome = output
        .stdout
        .lines()
        .rev()
        .find(|line| line.starts_with("PASS (") || line.starts_with("FAIL ("))?;
    Some(format!("{run}\n{outcome}"))
}

fn parse_case_output_stream(output: &str, stream_name: &str) -> anyhow::Result<HashMap<String, String>> {
    const START: &str = "VSCODE_TEST_OUTPUT_START\t";
    const END: &str = "VSCODE_TEST_OUTPUT_END\t";
    let mut cases = HashMap::new();
    let mut current: Option<(String, String)> = None;

    for raw_line in output.split_inclusive('\n') {
        let line = raw_line.strip_suffix('\n').unwrap_or(raw_line);
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(encoded_name) = line.strip_prefix(START) {
            if current.is_some() {
                return Err(anyhow!("nested VS Code test output marker in {stream_name}"));
            }
            current = Some((decode_hex(encoded_name)?, String::new()));
        } else if let Some(encoded_name) = line.strip_prefix(END) {
            let (case_name, case_output) = current
                .take()
                .ok_or_else(|| anyhow!("test output end marker without a start in {stream_name}"))?;
            let end_name = decode_hex(encoded_name)?;
            if end_name != case_name {
                return Err(anyhow!(
                    "test output end marker for {end_name:?} closes {case_name:?} in {stream_name}"
                ));
            }
            if cases.insert(case_name.clone(), case_output).is_some() {
                return Err(anyhow!("duplicate VS Code test output for {case_name:?} in {stream_name}"));
            }
        } else if let Some((_, case_output)) = &mut current {
            case_output.push_str(raw_line);
        }
    }

    if current.is_some() {
        return Err(anyhow!("test output start marker without an end in {stream_name}"));
    }
    Ok(cases)
}

fn execution_stream_output(stream: Option<&proto::ExecutionStream>) -> String {
    let bytes = stream.and_then(inline_bytes).unwrap_or_default();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn inline_bytes(stream: &proto::ExecutionStream) -> Option<Vec<u8>> {
    match stream.value.as_ref() {
        Some(execution_stream::Value::Inline(bytes)) => Some(bytes.clone()),
        None => None,
    }
}

fn duration_ms(duration: &ProtoDuration) -> u64 {
    (duration.seconds.max(0) as u64 * 1000) + (duration.nanos.max(0) as u64 / 1_000_000)
}

fn parse_listing(output: &str) -> anyhow::Result<Vec<ListedCase>> {
    let lines = output.lines().collect::<Vec<_>>();
    if lines.len() % 2 != 0 {
        return Err(anyhow!("test listing has {} lines; expected name/file:line pairs", lines.len()));
    }

    lines.chunks_exact(2).enumerate().map(|(index, pair)| {
        let name = pair[0];
        if name.is_empty() {
            return Err(anyhow!("test listing record {} has an empty test name", index + 1));
        }
        let (uri, line) = parse_location(pair[1]).ok_or_else(|| {
            anyhow!("test listing record {} has invalid location {:?}; expected file:line", index + 1, pair[1])
        })?;
        Ok(ListedCase { name: name.to_owned(), uri: Some(uri), line: Some(line) })
    }).collect()
}

fn parse_location(line: &str) -> Option<(String, u32)> {
    let (file, line_number) = line.rsplit_once(':')?;
    if file.is_empty() {
        return None;
    }
    Some((file.to_owned(), line_number.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{escape_doctest_filter, format_case_output, format_case_summary, normalize_lcov_paths, parse_case_outputs, parse_listing, parse_machine_results, resolve_prepared_path};

    #[test]
    fn parses_test_locations_listing() {
        let output = "First case\nsrc/first.cc:4\nSecond case\nmodules/second.cc:18\n";
        let cases = parse_listing(output).unwrap();
        assert_eq!(cases.iter().map(|case| &case.name).collect::<Vec<_>>(), vec!["First case", "Second case"]);
        assert_eq!(cases[1].uri.as_deref(), Some("modules/second.cc"));
        assert_eq!(cases[1].line, Some(18));
    }

    #[test]
    fn rejects_non_reporter_listing_output() {
        let error = parse_listing("[doctest] listing all test case names\n").unwrap_err();
        assert!(error.to_string().contains("expected name/file:line pairs"));
    }

    #[test]
    fn escapes_doctest_filter_separators() {
        assert_eq!(escape_doctest_filter(r#"case, with \ slash"#), r#"case\, with \\ slash"#);
    }

    #[test]
    fn parses_machine_results() {
        let results = parse_machine_results(
            "VSCODE_TEST_RESULT\t6669727374\tpassed\t12\nVSCODE_TEST_RESULT\t7365636f6e64\tfailed\t34\n",
        ).unwrap();
        assert_eq!(results["first"].status, "passed");
        assert_eq!(results["first"].duration_ms, Some(12));
        assert_eq!(results["second"].status, "failed");
    }

    #[test]
    fn keeps_each_startup_shared_case_output_separate() {
        let stdout = concat!(
            "runner setup\n",
            "VSCODE_TEST_OUTPUT_START\t6669727374\n",
            "first stdout\n",
            "VSCODE_TEST_OUTPUT_END\t6669727374\n",
            "VSCODE_TEST_RESULT\t6669727374\tpassed\t12\n",
            "VSCODE_TEST_OUTPUT_START\t7365636f6e64\n",
            "second stdout\n",
            "VSCODE_TEST_OUTPUT_END\t7365636f6e64\n",
            "VSCODE_TEST_RESULT\t7365636f6e64\tfailed\t34\n",
        );
        let stderr = concat!(
            "VSCODE_TEST_OUTPUT_START\t6669727374\n",
            "first stderr\n",
            "VSCODE_TEST_OUTPUT_END\t6669727374\n",
            "VSCODE_TEST_OUTPUT_START\t7365636f6e64\n",
            "second stderr\n",
            "VSCODE_TEST_OUTPUT_END\t7365636f6e64\n",
        );
        let outputs = parse_case_outputs(stdout, stderr).unwrap();
        assert_eq!(
            outputs["first"].stdout,
            "first stdout",
        );
        assert_eq!(
            outputs["first"].stderr,
            "first stderr",
        );
        assert_eq!(outputs["second"].stdout, "second stdout");
        assert_eq!(outputs["second"].stderr, "second stderr");
    }

    #[test]
    fn omits_empty_stream_sections_and_marker_padding() {
        let outputs = parse_case_outputs(
            "VSCODE_TEST_OUTPUT_START\t6669727374\n\nRUN case\nPASS (1 asserts passed)\n\nVSCODE_TEST_OUTPUT_END\t6669727374\n",
            "VSCODE_TEST_OUTPUT_START\t6669727374\n\nVSCODE_TEST_OUTPUT_END\t6669727374\n",
        )
        .unwrap();
        assert_eq!(
            format_case_output(&outputs["first"]),
            "---- STDOUT ----\nRUN case\nPASS (1 asserts passed)",
        );
        assert_eq!(
            format_case_summary(&outputs["first"]).as_deref(),
            Some("RUN case\nPASS (1 asserts passed)"),
        );
    }

    #[test]
    fn rejects_unclosed_case_output() {
        let error = parse_case_outputs(
            "VSCODE_TEST_OUTPUT_START\t6669727374\noutput\n",
            "",
        ).unwrap_err();
        assert!(error.to_string().contains("without an end"));
    }

    #[test]
    fn resolves_prepared_paths_against_the_prepared_working_directory() {
        assert_eq!(
            resolve_prepared_path("/workspace", "buck-out/profile.profraw"),
            PathBuf::from("/workspace/buck-out/profile.profraw"),
        );
        assert_eq!(
            resolve_prepared_path("/workspace", "/tmp/profile.profraw"),
            PathBuf::from("/tmp/profile.profraw"),
        );
    }

    #[test]
    fn normalizes_remote_source_paths_in_lcov() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let contents = b"SF:/some/remote/worker/work/tools/vscode-buck2/src/main.rs\nDA:1,1\nend_of_record\n";
        assert_eq!(
            String::from_utf8(normalize_lcov_paths(contents, &workspace_root)).unwrap(),
            format!("SF:{}/src/main.rs\nDA:1,1\nend_of_record\n", workspace_root.display()),
        );
    }
}
