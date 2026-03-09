# Claude Code Instructions — Rust Project

## Build & Check Strategy

**Batch edits before checking.** Do not run `cargo check`, `cargo build`, or `cargo clippy` after every individual file edit. Instead:

1. Complete all related edits across files first.
2. Run `cargo check` once after the full batch is done.
3. Fix any errors, then batch again before re-checking.

**Never run `cargo check` in a loop.** If a check fails, analyse the error output fully and apply all necessary fixes before running it again.

## Preferred Commands

| Purpose | Command |
|---|---|
| Verify code compiles | `cargo check` |
| Run tests | `cargo test` |
| Lint | `cargo clippy -- -D warnings` |
| Build (only when needed) | `cargo build` |
| Build release | `cargo build --release` |

- Prefer `cargo check` over `cargo build` during iteration — it skips codegen and is significantly faster.
- Only run `cargo build` when a binary or artifact is explicitly needed.
- Do not run `cargo check` and `cargo clippy` back-to-back unless both are necessary for the current task.

## Parallelism

If the machine appears under load, limit Cargo's job parallelism:

```bash
export CARGO_BUILD_JOBS=4
```

Or set it permanently in `.cargo/config.toml`:

```toml
[build]
jobs = 4
```

## Linker (Linux)

If `mold` is available, use it to speed up linking:

```toml
# .cargo/config.toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

## Workflow Rules

- **Do not `cargo check` between mechanical edits** (e.g., renaming a type across files, adding a field and updating all match arms). Finish the mechanical sweep, then check once.
- **Do not run the full test suite** to verify a small change compiles. Use `cargo check` or `cargo test <specific_test>`.
- **Avoid redundant tool invocations.** If `cargo check` just passed cleanly, do not run it again unless new edits have been made.
