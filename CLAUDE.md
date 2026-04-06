# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Valentine is a Python package for schema matching — finding relationships between columns of different tabular datasets. It implements 5 matching algorithms and provides metrics to evaluate match quality.

## Common Commands

```bash
# Install in development mode
pip install -e .

# Run all tests with coverage
coverage run --branch --source=valentine -m pytest -q tests && coverage xml

# Run a specific test file
pytest tests/test_valentine.py

# Run a single test
pytest tests/test_valentine.py::TestValentine::test_match

# Lint and format (Ruff, configured in pyproject.toml)
ruff check .
ruff format --check .
ruff format .

# Build & preview the documentation site (Zensical)
pip install -e ".[docs]"
zensical build --clean          # writes to ./site
zensical serve                  # live-reloading preview on http://localhost:8000
```

## Architecture

### Main API (`valentine/__init__.py`)

Single entry point:
- `valentine_match(dfs, matcher, df_names)` — match columns across all unique pairs of DataFrames

Both return `MatcherResults`, a dict subclass mapping `((table, col), (table, col)) → similarity_score`, sorted high-to-low. It provides `one_to_one()`, `take_top_n(n)`, `take_top_percent(p)`, and `get_metrics(ground_truth, metrics)`.

### Matching Algorithms (`valentine/algorithms/`)

All extend `BaseMatcher` (abstract base requiring `get_matches()`):

| Algorithm | Type | Notes |
|---|---|---|
| **Coma** | Schema + Instance | Pure Python COMA 3.0 implementation; trigram + TF-IDF matching |
| **Cupid** | Schema-only | Tree-based linguistic/structural matching |
| **DistributionBased** | Instance-only | Earth Mover's Distance on column value distributions |
| **JaccardDistanceMatcher** | Instance-only | Jaccard similarity with configurable string distance functions |
| **SimilarityFlooding** | Schema-only | Graph-based fixpoint computation |

### Data Sources (`valentine/data_sources/`)

`BaseTable`/`BaseColumn` abstractions with `DataframeTable`/`DataframeColumn` as concrete implementations. Includes automatic data type detection (varchar, int, float, date).

### Metrics (`valentine/metrics/`)

`Metric` base class with implementations: `Precision`, `Recall`, `F1Score`, `PrecisionTopNPercent`, `RecallAtSizeofGroundTruth`. Predefined sets: `METRICS_CORE`, `METRICS_ALL`.

## Key Constraints

- Python 3.10–3.14
- Ruff line length: 100 characters
- CI runs on Ubuntu/macOS/Windows across Python 3.10–3.14
- Coverage must not drop vs base commit (2% project threshold, 5% patch threshold)
