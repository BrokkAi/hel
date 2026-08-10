# Repository Guidelines

Unit tests are colocated in module-level `#[cfg(test)]` blocks. `tests/` holds only the PTY termination test and the `tests/e2e/` shell/expect harness.

## Coding Style & Naming Conventions

Use idiomatic Rust formatted by rustfmt. Prefer clear module boundaries that match the existing runtime/UI split. Name files and modules with `snake_case`; use `PascalCase` for types and enum variants, `snake_case` for functions and variables, and `SCREAMING_SNAKE_CASE` for constants. Keep comments short and useful, especially around async runtime behavior, terminal ownership, or protocol edge cases. Repository-facing text, code comments, and documentation should be written in English.

## Testing Guidelines

`.cargo/config.toml` defaults the build target to `x86_64-unknown-linux-musl` so the built controller doubles as the container worker. On non-x86_64-Linux hosts (for example macOS), pass your host triple explicitly: `cargo build --target aarch64-apple-darwin`.

Add focused unit tests near the code under test using `#[cfg(test)] mod tests`. Follow the existing descriptive test naming style, e.g. `autocomplete_updates_matches_for_prefix`. For state-machine changes, test the event transition or input handling directly rather than relying only on manual TUI checks. Run `cargo test` and `cargo clippy --all-targets -- -D warnings` before submitting changes.

## GitHub Authentication

Do not run `gh auth login` or ask the user to reauthenticate because a sandboxed authentication check failed. Run normal GitHub push and PR operations with escalated sandbox permissions; treat authentication as blocked only when the actual escalated operation returns an explicit authentication error.
