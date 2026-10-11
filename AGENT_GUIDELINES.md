# Karpathy Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" -> "Write tests for invalid inputs, then make them pass"
- "Fix the bug" -> "Write a test that reproduces it, then make it pass"
- "Refactor X" -> "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
3. [Step] -> verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.



---

# Environment Details (This Box)

## Operating System
- **Windows 11** (win32)
- PowerShell 5.1 (default shell)
- **Git Bash** available in userspace (`C:\Program Files\Git\bin\bash.exe`)

## Shell Commands

### PowerShell (Primary)
```powershell
# Navigation
Set-Location -LiteralPath "D:\PROJECTS\versakafka"
Get-Location

# File operations
Get-ChildItem -LiteralPath "D:\PROJECTS\versakafka" -Force
Test-Path -LiteralPath "path\to\file"

# Run commands
cargo check
cargo build --release
cargo test --workspace

# Git
git status
git add -A
git commit -m "message"

# Docker
docker-compose up -d
docker-compose down -v
docker-compose logs -f
```

### Git Bash (Available for Unix-like commands)
```bash
cd /d/PROJECTS/versakafka
ls -la
cargo check
make build
make test
make dev-up
```

## Key Paths
- **Project root**: `D:\PROJECTS\versakafka`
- **Cargo target**: `D:\PROJECTS\versakafka\target` (ignored by git)
- **Rust toolchain**: `C:\Users\xwm\.cargo\bin` (in PATH)
- **Rustup**: `C:\Users\xwm\.rustup`

## Cargo Commands (Run from Project Root)
```bash
# Check compilation
cargo check --workspace --all-targets

# Run tests
cargo test --workspace

# Build release
cargo build --workspace --release --bin kp-proxy

# Format & lint
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings

# Security audit
cargo audit
cargo deny check
```

## Docker Compose Services
```bash
# Start dev environment
docker-compose up -d

# Services:
# - kafka-1: localhost:9092 (broker), localhost:29092 (controller)
# - kafka-2: localhost:9094
# - kafka-3: localhost:9096
# - minio: localhost:9000 (API), localhost:9001 (console)
# - prometheus: localhost:9090
# - grafana: localhost:3000 (admin/admin)
# - jaeger: localhost:16686
# - toxiproxy: localhost:8474

# Stop
docker-compose down -v
```

## Useful PowerShell Patterns
```powershell
# Chain commands (use ; if you don't care about failure)
cmd1; cmd2

# Chain with dependency (cmd2 only if cmd1 succeeds)
cmd1; if ($?) { cmd2 }

# Quote paths with spaces
& "path with spaces\script.ps1"

# IMPORTANT: PowerShell 5.1 does NOT support && or || operators
# Use ; for sequential execution instead of &&
# Use cmd1; if ($?) { cmd2 } instead of cmd1 && cmd2
# Use cmd1; if (-not $?) { cmd2 } instead of cmd1 || cmd2
```

## Build Environment Notes (CRITICAL)
- **PowerShell 5.1 cannot run cargo build commands directly** - build scripts fail with "Access denied" (os error 5) on the target directory
- **Use Git Bash instead** (`C:\Program Files\Git\bin\bash.exe`) for all cargo commands
- **Set CARGO_TARGET_DIR to a temp path** (e.g., `/tmp/versakafka_target`) to avoid Windows permission issues
- **Add cargo to PATH** in the script: `export PATH="$PATH:/c/Users/xwm/.cargo/bin"`

Example working build script:
```bash
#!/bin/bash
cd /d/PROJECTS/versakafka
export PATH="$PATH:/c/Users/xwm/.cargo/bin"
export CARGO_TARGET_DIR="/tmp/versakafka_target"
mkdir -p "$CARGO_TARGET_DIR"
cargo check --workspace 2>&1
```

## Rust Toolchain
- **Channel**: stable (via rust-toolchain.toml)
- **Components**: rustfmt, clippy
- **Targets**: x86_64-unknown-linux-gnu, x86_64-pc-windows-msvc
- **MSRV**: 1.80

## Editor/IDE Notes
- **VS Code**: Use `rust-analyzer` extension
- **CLion/RustRover**: Works natively
- **Git Bash**: Available for Unix-like commands
- **Terminal**: Windows Terminal recommended

---

# Examples