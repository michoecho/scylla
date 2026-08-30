# Buck2 Test for VS Code

This directory contains two separate pieces:

* `buck2-test-executor` is an isolated Cargo project implementing Buck2's
  `test.v2_test_executor` protocol. Buck2 launches it for each `buck2 test`
  invocation; it asks Buck2 to execute the test binary for listing and each
  selected case, then reports discovery and results back through the protocol.
* `extension/` is the VS Code `TestController` client. It invokes `buck2 test`
  with the executor and reads the executor's JSON artifact.

The Rust helper is intentionally independent of the parent project's Buck2 and
Cargo graphs. Build it with:

```sh
cargo build --release --manifest-path tools/vscode-buck2/Cargo.toml
```

Then compile the extension separately:

```sh
cd tools/vscode-buck2/extension
npm install
npm run compile
```

To build and install the extension into the project-local VS Code, run:

```sh
./tools/vscode-buck2/build-and-install
```

The script builds the isolated Rust executor, packages it inside the VSIX,
compiles the extension, and installs the result with the project's `code`
wrapper. It does not modify a host-wide VS Code installation. Afterward, launch
the editor with `nix run .#code` (or `code` from the development shell) and open
the Testing view.

When developing from this checkout, the extension finds the release helper in
`../target/release`. The installed VSIX uses the executor bundled under its
`bin/` directory; `buck2Test.executorPath` can override either location.

The extension defaults to `buck2` on `PATH` and uses `//...` as its target
pattern. `buck2Test.buck2Path`, `buck2Test.executorPath`,
`buck2Test.targetPatterns`, `buck2Test.listArgument`,
`buck2Test.locationArgument`, and `buck2Test.caseArgument` can be set per
workspace. The default location argument selects this project's
`test-locations` reporter; set it to an empty string for test binaries that do
not provide that reporter.

For discovery, the extension runs `buck2 test <patterns> --` with
`--vscode-list-only`. The executor uses Buck2's test listing stage to ask each
test binary for its individual cases and writes them to the configured
artifact. Running selected VS Code cases writes the target/case pairs to a
temporary JSON selection file and passes its path to the executor. This keeps
large selections out of Buck2's argument vector. The executor filters the
listing against that file and writes its result to the artifact. For ordinary
targets, the full output from each Buck2 invocation is attached to its
corresponding VS Code test result.

Targets may set the `startup_shared` Buck label. For those targets, the
executor combines the selected doctest names into one `--test-case` filter and
runs the executable once. The bundled `vscode-results` doctest reporter emits
one machine-readable result per case and frames each case's output, so VS Code
still receives individual statuses, durations, and case-specific output.
Unlabelled targets retain one process invocation per case.

The `Debug Tests` profile does not run the test through Buck. The executor
asks Buck2 to materialise the test binary and resolve the command line, working
directory and environment it would have used (`--vscode-debug`, via Buck2's
`PrepareForLocalExecution`), and the extension hands that to a debug adapter
through `vscode.debug.startDebugging`. Cases are debugged one at a time.

The adapter is chosen by `buck2Test.debuggerType`, which defaults to `lldb`
(CodeLLDB, `vadimcn.vscode-lldb`). Set it to `cppdbg` to use the C/C++
extension's adapter (`ms-vscode.cpptools`), which can front either LLDB or GDB
via its `MIMode` key. Both extensions are already in this project's `flake.nix`
VS Code set. `buck2Test.debugConfiguration` is merged into the generated
configuration and overrides its keys, so adapter-specific settings such as
`{"MIMode": "gdb"}` or a `sourceMap` go there. Test binaries are built with
debug info and are not stripped, so no extra build mode is required.

The `Run Tests with Coverage` profile adds `--modifier root//:coverage` to the
Buck2 invocation. The executor assigns each test case an LLVM raw profile
output, merges the profiles with `llvm-profdata`, exports LCOV with `llvm-cov`,
and the extension loads the resulting files through VS Code's coverage API.
