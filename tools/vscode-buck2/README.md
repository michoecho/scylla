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
workspace. The default location argument selects this project's doctest
`test-locations` reporter; set it to an empty string for test binaries that do
not provide that reporter.

For discovery, the extension runs `buck2 test <patterns> --` with
`--vscode-list-only`. The executor uses Buck2's test listing stage to ask each
test binary for its individual cases and writes them to the configured
artifact. Running a VS Code case repeats the same protocol flow with
`--vscode-case` and writes its result to the artifact. The full Buck2 output is
attached to each corresponding VS Code test result.

The `Run Tests with Coverage` profile adds `--modifier root//:coverage` to the
Buck2 invocation. The executor assigns each test case an LLVM raw profile
output, merges the profiles with `llvm-profdata`, exports LCOV with `llvm-cov`,
and the extension loads the resulting files through VS Code's coverage API.
