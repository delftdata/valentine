# Contributing to Valentine

Thank you for your interest in contributing! This document explains how to get
involved, whether you're fixing a bug, adding a feature, or improving the docs.

## Getting started

1. **Fork** the repository and clone your fork.
2. Create a virtual environment and install in editable mode with all dev
   dependencies:

   ```shell
   pip install -e ".[dev]"
   ```

3. Create a branch for your change:

   ```shell
   git checkout -b my-feature
   ```

## Running the tests

```shell
pytest
```

Run with coverage:

```shell
pytest --cov=valentine --cov-report=term-missing
```

## Code style

Valentine uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.
Before opening a pull request, make sure your code passes:

```shell
ruff check .
ruff format --check .
```

You can auto-fix most issues with:

```shell
ruff check --fix .
ruff format .
```

## Opening a pull request

- Keep each PR focused on a single concern.
- Add or update tests for any behaviour change.
- Update the relevant documentation pages in `docs/` if needed.
- Reference any related issue in the PR description.

The CI pipeline will run tests on all supported Python versions (3.10–3.14)
across Linux, macOS, and Windows. PRs must pass CI before merging.

## Reporting bugs and requesting features

Please use [GitHub Issues](https://github.com/delftdata/valentine/issues).
Include a minimal reproducible example for bug reports and a clear motivation
for feature requests.
