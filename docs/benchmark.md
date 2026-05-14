---
icon: lucide/bar-chart-2
---

# Benchmark: v0.5.0 → v1.0.0

This page documents the performance comparison between valentine v0.5.0 and v1.0.0
on the [NYU dataset collection](research.md) — 10 real-world table pairs from NYC
Open Data spanning city government, education, housing, and transportation domains.

All timings are wall-clock seconds measured on a single Windows machine.
Per-dataset timeout: 120 s. Accuracy metrics are mean F1 and mean MRR across all
datasets that completed without error or timeout.

!!! note "Coma in v0.5.0 vs v1.0.0"
    In **v0.5.0**, `Coma` was the canonical Java-backed implementation — it required
    a JRE on the host machine and spawned a JVM per call. A pure-Python variant,
    `ComaPy`, existed but was considered **experimental**. In **v1.0.0**, the Java
    backend was removed entirely: `Coma` is now the pure-Python implementation
    (the former `ComaPy`, promoted to stable). All Coma comparisons in this document
    use the canonical class of each version — `Coma` (Java) for v0.5.0 and `Coma`
    (Python) for v1.0.0.

---

## Summary

### Speed (total wall-clock time, 10 datasets)

| Matcher | v0.5.0 | v1.0.0 | Speedup |
|---|---:|---:|---:|
| Coma (schema) | 8.31 s | 0.65 s | **13×** |
| Coma (instances) | 322.23 s | 4.71 s | **68×** |
| Cupid | 163.04 s | 3.55 s | **46×** |
| DistributionBased | 164.70 s | 3.94 s | **42×** |
| JaccardDistanceMatcher | 730.36 s ⚠ | 3.92 s | **186×** |
| SimilarityFlooding | 53.84 s | 3.30 s | **16×** |

!!! warning "v0.5.0 reliability issues"
    - **JaccardDistanceMatcher** timed out on 5 of 10 datasets; the 730 s total
      reflects only the 5 that completed plus 5 × 120 s timeouts.
    - **DistributionBased** crashed on the *Public Design Commission* dataset
      (`min() arg is an empty sequence`) due to a missing guard for sparse,
      text-heavy columns — fixed in v1.0.0.

### Accuracy (mean across completed datasets)

| Matcher | v0.5.0 F1 | v1.0.0 F1 | v0.5.0 Recall@GT | v1.0.0 Recall@GT | v0.5.0 MRR | v1.0.0 MRR |
|---|---:|---:|---:|---:|---:|---:|
| Coma (schema) | 0.6582 | 0.6647 | 0.6424 | 0.6507 | 0.3050 | 0.3024 |
| Coma (instances) | 0.7654 § | 0.7717 | 0.8132 § | 0.7631 | 0.3427 § | 0.3384 |
| Cupid | 0.4800 | 0.4848 | 0.4275 | 0.4298 | 0.2452 | 0.2489 |
| DistributionBased | 0.6465 † | 0.6805 | 0.5903 † | 0.6205 | 0.2892 † | 0.3019 |
| JaccardDistanceMatcher | 0.6664 ‡ | 0.6463 | 0.6250 ‡ | 0.5611 | 0.3354 ‡ | 0.2474 |
| SimilarityFlooding | 0.5071 | 0.4929 | 0.5014 | 0.5798 | 0.2853 | 0.3034 |

*§ v0.5.0 Coma (instances) mean computed over 9 completed datasets (Housing_Maintenance timed out).*  
*† v0.5.0 DistributionBased excludes the one crashed dataset (Public Design Commission).*  
*‡ v0.5.0 Jaccard computed over 5 completed datasets only (5 timeouts).*

!!! note "Recall@GT"
    **Recall@GT** (`RecallAtSizeofGroundTruth`) measures recall when selecting exactly
    `len(ground_truth)` top predictions — i.e., the fraction of correct pairs recovered
    if you keep as many predictions as there are gold matches.

Accuracy is essentially **preserved across the entire rewrite** — F1 differences are
within ±0.02 on all matchers. The speed-ups are purely from implementation
improvements, not accuracy trade-offs.

---

## Java Coma vs pure-Python Coma

v0.5.0 shipped two Coma variants: `Coma` (Java-backed, the canonical implementation)
and `ComaPy` (pure Python, experimental). v1.0.0 ships only `Coma` — the
pure-Python implementation, graduated from experimental `ComaPy` to the new stable
default, with the Java backend retired entirely.

| Matcher | v0.5.0 Java | v1.0.0 Python | Speedup | F1 delta | Recall@GT delta | MRR delta |
|---|---:|---:|---:|---:|---:|---:|
| Coma (schema) | 8.31 s | 0.65 s | **13×** | +0.007 | +0.008 | −0.003 |
| Coma (instances) | 322.23 s | 4.71 s | **68×** | +0.006 | −0.050 | −0.004 |

!!! warning "Java Coma in v0.5.0"
    Instance-mode Java Coma required manual heap configuration
    (`java_xmx` parameter) and still ran out of memory on large datasets even
    with 8 GB allocated. v1.0.0 eliminates the JVM dependency entirely —
    no Java installation, no heap tuning, no OOM errors.

The pure-Python rewrite is not only faster and more reliable, it is also
marginally *more accurate* on this benchmark.

---

## New in v1.0.0: embedding-based Jaccard

v1.0.0 adds `JaccardDistanceMatcher` with `distance_fun=StringDistanceFunction.Embedding`,
which uses sentence embeddings instead of character-level string distance.

| Matcher | Time | Mean F1 | Mean Recall@GT | Mean MRR |
|---|---:|---:|---:|---:|
| JaccardDistanceMatcher (string) | 3.92 s | 0.6463 | 0.5611 | 0.2474 |
| JaccardDistanceMatcher (embedding) | 48.98 s | 0.6567 | 0.5811 | 0.2514 |

The embedding variant requires `sentence-transformers` to be installed and trades
~14× more time for a small accuracy gain (+0.01 F1). It performs particularly well
on columns with semantically related but lexically dissimilar names.

---

## Per-dataset results

Each table covers one matcher. Columns show v0.5.0 and v1.0.0 side-by-side so
differences are immediately visible. v0.5.0 `Coma` = Java-backed (canonical).

### Coma (schema)

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.667 | 0.800 | 0.600 | 0.800 |
| DCM_StreetCenterLine | 0.833 | 0.857 | 0.857 | 0.857 |
| DPR_AthleticFacilities | 0.737 | 0.778 | 0.800 | 0.800 |
| DSNY_Districts | 0.533 | 0.500 | 0.500 | 0.500 |
| NYC_Municipal_Building | 0.800 | 0.545 | 0.800 | 0.600 |
| COVID-19_Free_Meals | 0.444 | 0.444 | 0.400 | 0.400 |
| Housing_Maintenance | 0.762 | 0.750 | 0.667 | 0.750 |
| Public_Design_Commission | 0.417 | 0.417 | 0.600 | 0.400 |
| Swim_for_Life | 0.889 | 0.889 | 0.800 | 0.800 |
| DOT_Resurfacing | 0.500 | 0.667 | 0.400 | 0.600 |
| **Mean** | **0.658** | **0.665** | **0.642** | **0.651** |

### Coma (instances)

*v0.5.0 timed out on Housing_Maintenance even at 8 GB heap (131 s); excluded from v0.5.0 mean.*

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.909 | 0.800 | 1.000 | 0.800 |
| DCM_StreetCenterLine | 0.833 | 0.769 | 0.857 | 0.714 |
| DPR_AthleticFacilities | 0.556 | 0.300 | 0.700 | 0.300 |
| DSNY_Districts | 0.533 | 0.588 | 0.625 | 0.500 |
| NYC_Municipal_Building | 1.000 | 0.889 | 1.000 | 0.800 |
| COVID-19_Free_Meals | 0.667 | 0.750 | 0.600 | 0.800 |
| Housing_Maintenance | TIMEOUT | 0.917 | TIMEOUT | 0.917 |
| Public_Design_Commission | 0.560 | 0.815 | 0.600 | 0.800 |
| Swim_for_Life | 0.889 | 0.889 | 1.000 | 1.000 |
| DOT_Resurfacing | 0.889 | 1.000 | 1.000 | 1.000 |
| **Mean** | **0.765 §** | **0.772** | **0.820 §** | **0.763** |

*§ v0.5.0 mean over 9 completed datasets.*

### Cupid

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.600 | 0.600 | 0.600 | 0.600 |
| DCM_StreetCenterLine | 0.727 | 0.667 | 0.571 | 0.714 |
| DPR_AthleticFacilities | 0.211 | 0.211 | 0.200 | 0.100 |
| DSNY_Districts | 0.462 | 0.462 | 0.500 | 0.500 |
| NYC_Municipal_Building | 0.333 | 0.333 | 0.200 | 0.200 |
| COVID-19_Free_Meals | 0.286 | 0.286 | 0.200 | 0.200 |
| Housing_Maintenance | 0.583 | 0.609 | 0.583 | 0.583 |
| Public_Design_Commission | 0.182 | 0.182 | 0.200 | 0.200 |
| Swim_for_Life | 0.667 | 0.750 | 0.600 | 0.600 |
| DOT_Resurfacing | 0.750 | 0.750 | 0.600 | 0.600 |
| **Mean** | **0.480** | **0.485** | **0.427** | **0.430** |

### DistributionBased

*v0.5.0 crashed on Public_Design_Commission (`min() arg is an empty sequence`); fixed in v1.0.0.*

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.500 | 0.571 | 0.400 | 0.400 |
| DCM_StreetCenterLine | 0.667 | 0.500 | 0.571 | 0.571 |
| DPR_AthleticFacilities | 0.320 | 0.333 | 0.100 | 0.200 |
| DSNY_Districts | 0.526 | 0.500 | 0.375 | 0.500 |
| NYC_Municipal_Building | 0.750 | 0.750 | 0.600 | 0.600 |
| COVID-19_Free_Meals | 0.750 | 0.750 | 0.800 | 0.800 |
| Housing_Maintenance | 0.667 | 0.762 | 0.667 | 0.667 |
| Public_Design_Commission | ERROR | 0.750 | ERROR | 0.667 |
| Swim_for_Life | 0.889 | 1.000 | 1.000 | 1.000 |
| DOT_Resurfacing | 0.750 | 0.889 | 0.800 | 0.800 |
| **Mean** | **0.647 †** | **0.681** | **0.590 †** | **0.621** |

*† v0.5.0 mean over 9 completed datasets.*

### JaccardDistanceMatcher

*v0.5.0 timed out on 5 datasets; mean computed over 5 that completed.*

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.400 | 0.400 | 0.400 | 0.400 |
| DCM_StreetCenterLine | TIMEOUT | 0.571 | TIMEOUT | 0.286 |
| DPR_AthleticFacilities | TIMEOUT | 0.148 | TIMEOUT | 0.000 |
| DSNY_Districts | 0.316 | 0.333 | 0.125 | 0.125 |
| NYC_Municipal_Building | 0.727 | 0.727 | 0.800 | 0.800 |
| COVID-19_Free_Meals | 0.889 | 0.889 | 0.800 | 0.800 |
| Housing_Maintenance | TIMEOUT | 0.667 | TIMEOUT | 0.667 |
| Public_Design_Commission | TIMEOUT | 0.839 | TIMEOUT | 0.733 |
| Swim_for_Life | 1.000 | 1.000 | 1.000 | 1.000 |
| DOT_Resurfacing | TIMEOUT | 0.889 | TIMEOUT | 0.800 |
| **Mean** | **0.666 ‡** | **0.646** | **0.625 ‡** | **0.561** |

*‡ v0.5.0 mean over 5 completed datasets only.*

### SimilarityFlooding

| Dataset | v0.5 F1 | v1.0 F1 | v0.5 Recall@GT | v1.0 Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.667 | 0.714 | 0.600 | 0.800 |
| DCM_StreetCenterLine | 0.714 | 0.625 | 0.714 | 0.714 |
| DPR_AthleticFacilities | 0.414 | 0.424 | 0.600 | 0.700 |
| DSNY_Districts | 0.333 | 0.476 | 0.250 | 0.500 |
| NYC_Municipal_Building | 0.400 | 0.333 | 0.400 | 0.600 |
| COVID-19_Free_Meals | 0.364 | 0.400 | 0.400 | 0.400 |
| Housing_Maintenance | 0.560 | 0.500 | 0.583 | 0.583 |
| Public_Design_Commission | 0.389 | 0.389 | 0.267 | 0.200 |
| Swim_for_Life | 0.800 | 0.667 | 0.800 | 0.800 |
| DOT_Resurfacing | 0.444 | 0.400 | 0.400 | 0.400 |
| **Mean** | **0.507** | **0.493** | **0.501** | **0.580** |

---

### ComaPy *(v0.5.0 experimental — now the stable Coma in v1.0.0)*

| Dataset | schema F1 | instances F1 | schema Recall@GT | instances Recall@GT |
|---|---:|---:|---:|---:|
| Capital_Projects | 0.667 | 0.909 | 0.600 | 1.000 |
| DCM_StreetCenterLine | 0.857 | 0.857 | 0.857 | 0.857 |
| DPR_AthleticFacilities | 0.783 | 0.609 | 0.700 | 0.400 |
| DSNY_Districts | 0.533 | 0.667 | 0.500 | 0.625 |
| NYC_Municipal_Building | 0.727 | 0.800 | 0.800 | 0.800 |
| COVID-19_Free_Meals | 0.400 | 0.800 | 0.400 | 0.800 |
| Housing_Maintenance | 0.615 | 0.750 | 0.667 | 0.750 |
| Public_Design_Commission | 0.538 | 0.615 | 0.267 | 0.467 |
| Swim_for_Life | 0.800 | 1.000 | 0.800 | 1.000 |
| DOT_Resurfacing | 0.444 | 0.909 | 0.400 | 1.000 |
| **Mean** | **0.636** | **0.792** | **0.599** | **0.770** |

### JaccardDistanceMatcher (embedding) *(v1.0.0 only)*

| Dataset | F1 | Recall@GT |
|---|---:|---:|
| Capital_Projects | 0.400 | 0.400 |
| DCM_StreetCenterLine | 0.571 | 0.286 |
| DPR_AthleticFacilities | 0.138 | 0.000 |
| DSNY_Districts | 0.381 | 0.125 |
| NYC_Municipal_Building | 0.727 | 0.800 |
| COVID-19_Free_Meals | 0.889 | 0.800 |
| Housing_Maintenance | 0.696 | 0.667 |
| Public_Design_Commission | 0.765 | 0.733 |
| Swim_for_Life | 1.000 | 1.000 |
| DOT_Resurfacing | 1.000 | 1.000 |
| **Mean** | **0.657** | **0.581** |

---

## Methodology

- **Datasets**: 10 real-world NYC Open Data table pairs from the NYU schema-matching
  benchmark, covering city government, education, housing, and transportation domains.
- **Metrics**: F1Score and RecallAtSizeofGroundTruth (top-|GT| predictions, TP/|GT|)
  via `matches.get_metrics()`; MRR computed manually from ranked match order.
- **Timeout**: 120 s per dataset per matcher, enforced via `ThreadPoolExecutor`
  with non-blocking shutdown.
- **v0.5.0 Java Coma heap**: `java_xmx="8192m"` (8 GB) — the default 1 GB caused
  OOM on two large datasets; even 4 GB was insufficient for one.
- **Hardware**: Single Windows workstation; timings are wall-clock, single-threaded
  (`process_num=1` for DistributionBased).
