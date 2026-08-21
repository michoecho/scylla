use std::{
    collections::HashSet,
    fs,
    io,
    path::PathBuf,
    pin::Pin,
    process,
    task::{Context, Poll},
};

use anyhow::{anyhow, Context as _};
use clap::Parser;
use prost_types::Duration as ProtoDuration;
use serde::Serialize;
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
    selected_cases: HashSet<String>,
    list_arg: String,
    location_arg: Option<String>,
    case_arg: String,
}

#[derive(Debug, Serialize)]
struct OutputFile {
    tests: Vec<DiscoveredCase>,
    results: Vec<CaseResult>,
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
    let mut output = OutputFile { tests: Vec::new(), results: Vec::new() };
    let mut exit_code = 0;

    while let Some(message) = spec_receiver.recv().await {
        let Some(spec) = message else { break };
        match process_spec(&mut orchestrator, &options, spec, &mut output).await {
            Ok(()) => {}
            Err(error) => {
                eprintln!("buck2-vscode-test-executor: {error:#}");
                exit_code = 32;
            }
        }
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
            "--vscode-case" => {
                options.selected_cases.insert(value_or_next(args, index, value)?.to_owned());
            }
            "--vscode-list-arg" => options.list_arg = value_or_next(args, index, value)?.to_owned(),
            "--vscode-location-arg" => {
                let location_arg = value_or_next(args, index, value)?;
                options.location_arg = (!location_arg.is_empty()).then(|| location_arg.to_owned());
            }
            "--vscode-case-arg" => options.case_arg = value_or_next(args, index, value)?.to_owned(),
            _ => {}
        }
        if value.is_empty() && matches!(key, "--vscode-output" | "--vscode-case" | "--vscode-list-arg" | "--vscode-location-arg" | "--vscode-case-arg") {
            index += 1;
        }
        index += 1;
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
        TestStage { item: Some(test_stage::Item::Listing(test_stage::Listing { suite: target_name.clone(), cacheable: false })) },
        list_command,
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
        cases.into_iter().filter(|case| options.selected_cases.contains(&case.name)).collect()
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

    for case in cases {
        let case_name = case.name;
        let case_filter = if options.case_arg == "--test-case={}" {
            escape_doctest_filter(&case_name)
        } else {
            case_name.clone()
        };
        let command = command_with_arg(&spec.command, &options.case_arg.replace("{}", &case_filter));
        let result = execute(
            orchestrator,
            &handle,
            &spec,
            TestStage { item: Some(test_stage::Item::Testing(Testing { suite: target_name.clone(), testcases: vec![case_name.clone()], variant: None, repeat_count: None })) },
            command,
        ).await?;
        let details = execution_output(&result);
        let status = result_status(&result);
        orchestrator.report_test_result(proto::ReportTestResultRequest { result: Some(TestResult {
            name: case_name.clone(),
            status: status.0,
            msg: None,
            target: Some(handle.clone()),
            duration: result.execution_time.clone(),
            details: details.clone(),
            max_memory_used_bytes: result.max_memory_used_bytes,
        }) }).await?;
        output.results.push(CaseResult {
            target: target_name.clone(),
            case_name,
            status: status.1,
            duration_ms: result.execution_time.as_ref().map(duration_ms),
            output: details,
        });
    }
    Ok(())
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

async fn execute(
    orchestrator: &mut test_orchestrator_client::TestOrchestratorClient<Channel>,
    handle: &ConfiguredTargetHandle,
    spec: &ExternalRunnerSpec,
    stage: TestStage,
    command: Vec<ArgValue>,
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
            env: spec.env.iter().map(|(key, value)| proto::EnvironmentVariable {
                key: key.clone(),
                value: Some(ArgValue {
                    content: Some(ArgValueContent { value: Some(proto::arg_value_content::Value::SpecValue(value.clone())) }),
                    format: None,
                }),
            }).collect(),
        }),
        executor_override: None,
        required_local_resources: Vec::new(),
        disable_test_execution_caching: true,
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
    format!("---- STDOUT ----\n{}\n---- STDERR ----\n{}", execution_stream_output(result.stdout.as_ref()), execution_stream_output(result.stderr.as_ref()))
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
    use super::{escape_doctest_filter, parse_listing};

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
}
