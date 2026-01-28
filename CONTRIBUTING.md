# Contributing to abx

Thanks for your interest in contributing to **abx**! This document explains how to set up the project locally, how to propose changes, and the standards we follow.

---

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Project structure](#project-structure)
- [Local development setup](#local-development-setup)
- [Running the CLI locally](#running-the-cli-locally)
- [Testing](#testing)
- [Style and quality standards](#style-and-quality-standards)
- [Documentation standards](#documentation-standards)
- [Submitting changes](#submitting-changes)
- [Reporting bugs](#reporting-bugs)
- [Feature requests](#feature-requests)
- [Security issues](#security-issues)

---

## Code of conduct

Be respectful, constructive, and kind.
We want this project to be friendly for contributors of all experience levels.

---

## Project structure

Recommended structure:

```
abx/
  README.md
  CHANGELOG.md
  CONTRIBUTING.md
  LICENSE
  SECURITY.md

  docs/
    convert.md
    config.md
    data-contract.md
    troubleshooting.md

  src/
    abx/
      ...package code...
```

---

## Local development setup

### 1) Create a virtual environment

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install in editable mode

```bash
python -m pip install -U pip
python -m pip install -e .
```

Optional (recommended for Parquet support):
```bash
python -m pip install pyarrow
```

---

## Running the CLI locally

Once installed editable:

```bash
abx --help
abx convert --help
abx convert unit --help
abx convert events --help
```

If you don’t have a console script entrypoint yet, you can also run the module form (depending on packaging):

```bash
python -m abx --help
```

---

## Testing

### Minimal sanity checks (manual)
Before submitting changes, please run:

- `abx convert unit --preview` on a small unit dataset
- `abx convert events --preview` on a small events dataset
- at least one value-based metric (`--value` + `sum_value`)
- at least one exposure-anchored run (`--exposure` + `--window`)

### Automated tests (recommended, if present)
If the repo includes `pytest` tests:

```bash
pytest -q
```

If tests are not implemented yet, contributions adding tests are very welcome.

---

## Style and quality standards

### Code style
- Prefer clear, readable code over “clever” code.
- Fail fast with actionable error messages (`SystemExit` with a good explanation is acceptable for CLI failures).
- Keep CLI parsing and conversion logic separated when possible.

### Backwards compatibility
If changing CLI behavior:
- update documentation
- consider whether existing configs might break
- document breaking changes in `CHANGELOG.md` under “Changed” or “Removed”

### Performance
- Avoid unnecessary copies of large DataFrames.
- Prefer vectorized pandas operations over Python loops.
- Use `--preview` and minimal printing for large datasets.

---

## Documentation standards

- Docs must match the actual behavior of the code.
- `docs/convert.md` is the authoritative reference for CLI arguments and metric rules.
- Examples should be copy-pastable.
- If adding a new metric rule:
  - document it in `docs/convert.md`
  - add at least one example command
  - add tests (recommended)

---

## Submitting changes

1. Fork the repo (or create a branch if you have access)
2. Create a feature branch:
   - `feat/<short-name>` for features
   - `fix/<short-name>` for bug fixes
   - `docs/<short-name>` for documentation-only changes
3. Make changes with clear commits
4. Update docs and changelog if needed
5. Open a pull request

PRs should include:
- what changed
- why it changed
- how to test it
- any known limitations

---

## Reporting bugs

Please include:
- your OS (Windows/macOS/Linux)
- Python version (`python --version`)
- pandas version (`python -c "import pandas as pd; print(pd.__version__)"`)
- the command you ran
- the full error message
- a minimal dataset (5–20 rows) if possible

This makes issues reproducible and fast to fix.

---

## Feature requests

Open an issue with:
- the problem you’re solving
- a proposed CLI design (flags/options)
- expected inputs/outputs
- examples of real datasets (anonymized if needed)

---

## Security issues

If you believe you found a security issue, do **not** open a public issue.
Please follow `SECURITY.md`.

---

Thanks again for contributing!
