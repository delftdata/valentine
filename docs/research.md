---
icon: lucide/graduation-cap
---

# Research

Valentine started as a research project at [Delft Data][delftdata], the
data-management research group at TU Delft. It was first released alongside
the ICDE 2021 paper *Valentine: Evaluating Matching Techniques for Dataset
Discovery*, which introduced both the matching benchmark and the evaluation
methodology that the package still implements today.

  [delftdata]: https://www.wis.ewi.tudelft.nl/data-management

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

## Datasets

Valentine offers a wide spectrum of dataset pairs with ground truth
containing valid matches among their corresponding columns. These
dataset pairs have been fabricated by Valentine's dataset relatedness
scenario generator. The ICDE 2021 paper classifies relatedness of two
datasets into four categories:

| Category                  | Description                                                                                            |
|---------------------------|--------------------------------------------------------------------------------------------------------|
| **Unionable**             | Tables that describe the same entity and can be stacked vertically.                                    |
| **View-unionable**        | Tables derived from the same source via different projections/selections — unionable after alignment. |
| **Joinable**              | Tables that can be combined via a shared key.                                                          |
| **Semantically-joinable** | Tables whose keys are not literally equal but semantically refer to the same entities.                |

The datasets used in the paper are
[hosted on Zenodo](https://zenodo.org/record/5084605#.YOgWHBMzY-Q) with
DOI: **10.5281/zenodo.5084605**. The table below lists the dataset
sources and dedicated links to the corresponding fabricated dataset
pairs per relatedness scenario, along with the min/max number of rows
and columns of the fabricated datasets.

| Dataset Source                                                        | #Pairs |    #Rows     | #Columns |                                                                                                                                                                                                                                                                                                                                                                                                                        Links                                                                                                                                                                                                                                                                                                                                                                                                                         |
|-----------------------------------------------------------------------|:------:|:------------:|:--------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| [TPC-DI](http://www.vldb.org/pvldb/vol7/p1367-poess.pdf)              |  180   | 7 492–14 983 |  11–22   |              [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FView-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FSemantically-Joinable)              |
| [Open Data](http://www.vldb.org/pvldb/vol11/p813-nargesian.pdf)       |  180   | 11 628–23 255 |  26–51   |                   [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FView%20-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FSemantically-Joinable)                   |
| [ChEMBL](https://www.ebi.ac.uk/chembl/)                               |  180   | 7 500–15 000 |  12–23   |                              [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FView-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FSemantically-Joinable)                              |
| [WikiData](https://www.wikidata.org)                                  |   4    | 5 423–10 846 |  13–20   | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_unionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_viewunion), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_joinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_semjoinable) |
| [Magellan Data](https://sites.google.com/site/anhaidgroup/useful-stuff/data) |   7    | 864–131 099  |   3–7    |                                                                                                                                                                                                                                                                                        [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FDeepMDatasets)                                                                                                                                                                                                                                                                                        |

### Filename conventions

The filenames of the fabricated datasets encode the scenario parameters:

- ***ac*** / ***ec*** — dataset pairs with *noisy* or *verbatim*
  schemata, respectively.
- ***av*** / ***ev*** — dataset pairs with *noisy* or *verbatim*
  instances.
- ***horizontal_p*** — datasets derived from a horizontal split with
  `p%` row overlap based on the original dataset.
- ***vertical_p*** — datasets derived from a vertical split with `p%`
  column overlap based on the original dataset.
- ***both_p1_p2*** — datasets derived from both a horizontal split
  (`p1%` row overlap) and a vertical split (`p2%` column overlap).

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

[:material-file-document: Read the paper][paper] ·
[:material-youtube: ICDE 2021 presentation][icde-video] (Christos
Koutras)

  [paper]: https://ieeexplore.ieee.org/abstract/document/9458921
  [icde-video]: https://www.youtube.com/watch?v=lk9gYF4G758

[![ICDE 2021 presentation](https://img.youtube.com/vi/lk9gYF4G758/0.jpg)](https://www.youtube.com/watch?v=lk9gYF4G758)

??? note "BibTeX"

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

[:material-file-document: Read the paper][vldb-paper] ·
[:material-youtube: VLDB 2021 demonstration][vldb-video] (Kyriakos
Psarakis)

  [vldb-paper]: https://www.vldb.org/pvldb/vol14/p2871-koutras.pdf
  [vldb-video]: https://www.youtube.com/watch?v=EOwD-kHuAkI

[![VLDB 2021 demonstration](https://img.youtube.com/vi/EOwD-kHuAkI/0.jpg)](https://www.youtube.com/watch?v=EOwD-kHuAkI)

??? note "BibTeX"

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
benchmark data generators, the GUI, and the dataset fabricator — is
preserved on the [`v1.1` tag of the repository][v11]. Use it if you
want to reproduce the paper's numbers exactly; use the current
`master` for new work.

  [v11]: https://github.com/delftdata/valentine/tree/v1.1

## Matchers not in the current package

The research suite evaluated **seven** matching methods in total. Two
embedding-based methods were part of the original benchmark but are not
maintained in the current Python package. They remain available in the
`v1.1` snapshot for reproducibility.

| Method       | Paper                                                                                                                                        |
|--------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| **EmbDI**    | Cappuzzo, R., Papotti, P., Thirumuruganathan, S. *Creating Embeddings of Heterogeneous Relational Datasets for Data Integration Tasks.* SIGMOD 2020. |
| **SemProp**  | Fernandez, R.C., Mansour, E., Qahtan, A.A., Elmagarmid, A., Ilyas, I., Madden, S., Ouzzani, M., Stonebraker, M., Tang, N. *Seeping Semantics: Linking Datasets Using Word Embeddings for Data Discovery.* ICDE 2018. |

## Citing Valentine

If Valentine is useful in your research, please cite the ICDE paper (and
optionally the VLDB demo). The BibTeX entries above are ready to drop into
your bibliography.
