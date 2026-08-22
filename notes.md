# Notes

## Restrict AFL instrumentation to one module

Trims coverage instrumentation to the matching sources; the persistent-mode
forkserver stays regardless, so a wrong glob fuzzes fine but never finds
anything.

```sh
echo 'src:*/modules/test_rng/*' > /tmp/allow.txt
AFL_LLVM_ALLOWLIST=/tmp/allow.txt cmake --build --preset Fuzz
```

## Generate `compile_commands.json`

```sh
ln -sf $(buck2 bxl prelude//cxx/tools/compilation_database.bxl:generate -- --targets //...) $(git rev-parse --show-toplevel)
```
