---
name: write-code
description: "Develop a feature or fix a bug in Python code. Use when: implementing new functionality, fixing a bug, writing production code, adding tests. Follows project folder architecture, code standards, type hints, and runs pre-commit checks."
argument-hint: "Describe the feature to implement or the bug to fix"
---

# Write Code

Implement a feature or fix a bug following this project's architecture, code standards, and quality checks.

## When to Use

- Implement a new feature
- Fix a bug
- Refactor existing code
- Any change that touches `src/` or `tests/`

## Procedure

### 1. Understand the Task

- Read the user's request carefully.
- Identify which part of the codebase is affected.
- If the request is ambiguous, ask one round of clarifying questions before proceeding.

### 2. Explore the Codebase

- Read relevant files in `src/` and `tests/` to understand existing patterns.
- Identify where new code should live based on the folder architecture:

| Folder | Purpose |
|--------|---------|
| `src/app/` | Application logic (use cases, orchestration) |
| `src/utils/` | Pure utility functions, helpers |
| `src/infra/io` | Infrastructure: file systems |
| `src/infra/database` | Infrastructure: databases |
| `src/infra/http` | Infrastructure: HTTP requests, APIs |
| `tests/` | All tests (mirror `src/` structure) |

### 3. Write the Code

Follow these standards strictly:

- **Type hints** on every function signature (parameters and return type).
- **Docstrings** on every new function and class.
- **No time-specific language** in comments — avoid "now", "currently", "today".
- Keep functions focused and small.
- Place code in the correct folder per the architecture table above.

### 4. Write Tests

- Create or update test files in `tests/`, mirroring the `src/` structure.
  - Code in `src/app/feature.py` → tests in `tests/app/test_feature.py`
  - Code in `src/utils/helpers.py` → tests in `tests/utils/test_helpers.py`
- Use **pytest** conventions: `test_` prefix for functions, descriptive names.
- Cover the main path and at least one edge case or error case.
- Run tests via the Makefile:

```bash
make test
```

### 5. Type Check with MyPy

Run mypy through pre-commit to ensure type compliance:

```bash
pre-commit run mypy --all-files
```

- Fix all mypy errors before considering the task done.
- If mypy reports errors in files you did not modify, flag them to the user but do not fix unrelated code.

### 6. Run Full Pre-commit

Run all pre-commit hooks to catch formatting and linting issues:

```bash
pre-commit run --all-files
```

- Fix any errors reported by ruff or ruff-format.
- Re-run until all hooks pass.

### 7. Update Documentation

- If the change is user-facing, create or update relevant files in `docs/`.
- Ensure new public functions have docstrings.

## Completion Checklist

- [ ] Code placed in the correct `src/` subfolder
- [ ] Type hints on all function signatures
- [ ] Docstrings on new functions/classes
- [ ] Tests written in `tests/` with matching structure
- [ ] `make test` passes
- [ ] `pre-commit run mypy --all-files` passes
- [ ] `pre-commit run --all-files` passes
- [ ] Documentation updated (if user-facing change)
