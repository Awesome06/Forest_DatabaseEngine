# Workspace Agent Directives

## Bootstrap Protocol
- On session initialization:
  1. Verify Git hook configuration by checking if `.git/hooks/pre-commit` exists. If missing, execute `.agent/setup.ps1` (or verify that `.agent/.registry` is present).
  2. Check if the local `.agent/skills/` directory contains active domain skills.
  3. If missing or empty, invoke the global `workspace-initializer` skill with the stack detected from repository files.
  4. Once skills are linked, strictly enforce their constraints during all subsequent code generation and review tasks.
  5. Note: Global skills (`code-review`, `doc-sync`, `error-discipline`, `git-release`, `security-auditor`, `test-discipline`, `workspace-initializer`) are pre-loaded at the machine level (`~/.gemini/config/skills/`). Do not attempt to symlink them into `.agent/skills/`.

## General Constraints
- Do not add or remove third-party dependencies without explicit user confirmation.
- Keep the context window lean: do not load files into context unless actively modifying them.
- Run active linters and tests via `test-discipline` before staging any changes.
- Never run `git commit` or `git push` without explicit user instruction. Always leave changes uncommitted for user review.
