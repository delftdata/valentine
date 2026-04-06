---
icon: lucide/graduation-cap
---

# Research

Valentine started as a research project at [Delft Data][delftdata], the
data-management research group at TU Delft. It was first released alongside
the ICDE 2021 paper *Valentine: Evaluating Matching Techniques for Dataset
Discovery*, which introduced both the matching benchmark and the evaluation
methodology that the package still implements today.

  [delftdata]: https://delftdata.github.io/

## Overview

Valentine is an open-source framework designed to execute large-scale
automated matching processes on tabular data. The system implements
established schema-matching methodologies and provides tools for evaluation
and real-world deployment in data lakes.

The original research project shipped two main capabilities beyond the
matching algorithms themselves:

- **A dataset fabricator** — a tool that generates evaluation dataset pairs
  respecting specific relational semantics (unionable, view-unionable,
  joinable, semantically-joinable), so that matchers can be compared on
  workloads with a known ground truth.
- **A GUI for evaluating schema matching methods** — an interactive tool
  that lets researchers run matchers, inspect results, and compute metrics
  on the fabricated benchmarks.

## Dataset categories

The Valentine benchmark defines four relational scenarios used to fabricate
evaluation pairs:

| Category                | Description                                                                                         |
|-------------------------|-----------------------------------------------------------------------------------------------------|
| **Unionable**           | Tables that describe the same entity and can be stacked vertically.                                 |
| **View-unionable**      | Tables derived from the same source via different projections/selections — unionable after alignment. |
| **Joinable**            | Tables that can be combined via a shared key.                                                       |
| **Semantically-joinable** | Tables whose keys are not literally equal but semantically refer to the same entities.            |

## Data sources

The fabricator draws from a range of real-world data sources used across
the ICDE 2021 evaluation:

- **TPC-DI** — data-integration benchmark schemas
- **Open Data** portals
- **ChEMBL** — bioactivity database
- **WikiData** — collaborative knowledge graph
- **Magellan Data** — entity-matching benchmark collection

## Papers

### Valentine: Evaluating Matching Techniques for Dataset Discovery

The original paper proposes Valentine as an extensible experimental suite
for comparing schema matching techniques on dataset-discovery workloads.
It formalizes the evaluation protocol (precision, recall, F1 at different
cutoffs) and benchmarks COMA, Cupid, Similarity Flooding, Distribution-Based,
and Jaccard-based matchers across a range of real-world fabrication
scenarios.

> Koutras, C., Siachamis, G., Ionescu, A., Psarakis, K., Brons, J.,
> Fragkoulis, M., Lofi, C., Bonifati, A., Katsifodimos, A. *Valentine:
> Evaluating Matching Techniques for Dataset Discovery.* ICDE 2021.

[:material-file-document: Read the paper][paper]

  [paper]: https://ieeexplore.ieee.org/abstract/document/9458921

```bibtex
@inproceedings{koutras2021valentine,
  title={Valentine: Evaluating Matching Techniques for Dataset Discovery},
  author={Koutras, Christos and Siachamis, George and Ionescu, Andra and
          Psarakis, Kyriakos and Brons, Jerry and Fragkoulis, Marios and
          Lofi, Christoph and Bonifati, Angela and Katsifodimos, Asterios},
  booktitle={2021 IEEE 37th International Conference on Data Engineering (ICDE)},
  pages={468--479},
  year={2021},
  organization={IEEE}
}
```

### Valentine in Action: Matching Tabular Data at Scale

A VLDB 2021 demo paper showing Valentine in action on larger, more diverse
table collections and introducing the interactive tooling built around the
library.

> Koutras, C., Psarakis, K., Siachamis, G., Ionescu, A., Fragkoulis, M.,
> Bonifati, A., Katsifodimos, A. *Valentine in Action: Matching Tabular
> Data at Scale.* VLDB 2021 (Demo).

```bibtex
@article{koutras2021demo,
  title={Valentine in Action: Matching Tabular Data at Scale},
  author={Koutras, Christos and Psarakis, Kyriakos and Siachamis, George and
          Ionescu, Andra and Fragkoulis, Marios and Bonifati, Angela and
          Katsifodimos, Asterios},
  journal={VLDB},
  volume={14},
  number={12},
  pages={2871--2874},
  year={2021},
  publisher={VLDB Endowment}
}
```

## Algorithms & references

Valentine ships pure-Python implementations of several well-known schema-
matching techniques. The table below links each matcher to the paper it is
based on.

| Matcher                   | Paper                                                                                                                       |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `Coma`                    | Do, H.H., Rahm, E. *COMA: A System for Flexible Combination of Schema Matching Approaches.* VLDB 2002.                     |
| `Cupid`                   | Madhavan, J., Bernstein, P.A., Rahm, E. [*Generic Schema Matching with Cupid.*][cupid] VLDB 2001.                          |
| `DistributionBased`       | Zhang, M., Hadjieleftheriou, M., Ooi, B.C., Procopiuc, C.M., Srivastava, D. [*Automatic Discovery of Attributes in Relational Databases.*][zhang] SIGMOD 2011. |
| `JaccardDistanceMatcher`  | Baseline using Jaccard similarity over column value sets, with a configurable string distance for element equality.         |
| `SimilarityFlooding`      | Melnik, S., Garcia-Molina, H., Rahm, E. [*Similarity Flooding: A Versatile Graph Matching Algorithm and its Application to Schema Matching.*][sf] ICDE 2002. |

  [cupid]: https://www.vldb.org/conf/2001/P049.pdf
  [zhang]: https://dl.acm.org/doi/10.1145/1989323.1989336
  [sf]: https://ieeexplore.ieee.org/document/994702

## Experimental suite

The original experimental suite from the ICDE paper — including the
benchmark data generators used for the evaluation — is preserved on the
[`v1.1` tag of the repository][v11]. Use it if you want to reproduce the
paper's numbers exactly; use the current `master` for new work.

  [v11]: https://github.com/delftdata/valentine/tree/v1.1

## Citing Valentine

If Valentine is useful in your research, please cite the ICDE paper (and
optionally the VLDB demo). The BibTeX entries above are ready to drop into
your bibliography.
