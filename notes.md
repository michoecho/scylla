# Notes

## Generate `compile_commands.json`

```sh
ln -sf $(buck2 bxl prelude//cxx/tools/compilation_database.bxl:generate -- --targets //...) $(git rev-parse --show-toplevel)
```
