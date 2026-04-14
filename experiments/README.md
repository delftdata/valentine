# Experiments

## Benchmark harness

`bench.py` is the benchmark harness used for performance and accuracy regression testing.

```bash
# Quick synthetic suite (used in CI)
python experiments/bench.py --quick --baseline experiments/bench_baseline.json --accuracy-only

# Full suite against NYU data
python experiments/bench.py --full

# Local timing comparison (30% threshold)
python experiments/bench.py --quick --baseline experiments/bench_baseline.json

# Update baseline after intentional accuracy changes
python experiments/bench.py --quick --baseline experiments/bench_baseline.json --update-baseline

# Profile individual matchers (requires pyinstrument)
python experiments/bench.py --quick --profile
```

`bench_baseline.json` stores the expected F1 scores and match counts for the
deterministic synthetic suite. CI uses `--accuracy-only` to block PRs that
change matching behaviour without updating the baseline.

If your PR intentionally changes algorithm behaviour (improvements or
refactors), the bench CI will fail and tell you to run `--update-baseline`.
Commit the updated `bench_baseline.json` with your PR.

## NYU experiment data

The dataset pairs under `data/` were prepared by
[Yurong Liu](https://github.com/lyrain2001). They are sourced from
NYC Open Data with schema and value perturbations based on LLMs to create
pairs of diverse matching difficulty levels. Each subdirectory contains a
`source_table.csv`, `target_table.csv`, and `ground_truth.json`.

Thank you, Yurong, for curating and contributing these datasets!

`experiment_nyu.py` runs all five Valentine matchers against the NYU data and
prints per-dataset metrics.
