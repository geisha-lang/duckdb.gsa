# AGENTS.md — Maintenance & PR Rules (`duckdb.gsa`)

Native DuckDB 2.x bindings and composable Query DSL (`duckdb.duckdb`, `duckdb.dsl`) for Geisha.

## Branch, Worktree & Pull Request Workflow (No Direct Push to `main`)

`main` is protected on GitHub and gated by `Test (linux-x86_64)` and `Test (darwin-arm64)`.

1. **Never edit or commit directly on `main` in `~/code/duckdb.gsa`**:
   - Keep `~/code/duckdb.gsa` strictly on clean `main` (`git pull --ff-only`).
   - Always open an isolated `git worktree` on a feature/fix branch (`git -C ~/code/duckdb.gsa worktree add -b <type>/<topic> <workspace>/duckdb-wt origin/main`).
2. **Open a Pull Request & wait for CI before merging**:
   - Push your branch (`git push -u origin <branch>`), open a PR via `gh pr create`, and merge only after both `Test (linux-x86_64)` and `Test (darwin-arm64)` pass.
   - Do not self-merge breaking public API changes without user review.

## Engineering Rules & Verification

- Keep platform-specific DuckDB header/library search paths inside `[[target.native.when]]` (`os = "macos"` and `os = "linux"`) in `geisha.toml`.
- Note that `geisha-lang/tabular.gsa` pins `duckdb.gsa` by commit + `sha256` in `geisha.lock`; bump `version` in `geisha.toml` when publishing new features intended for downstream MVS upgrades.
- Verify locally before opening a PR:
  ```sh
  MACOSX_DEPLOYMENT_TARGET=14.0 geisha check
  MACOSX_DEPLOYMENT_TARGET=14.0 geisha test
  ```
