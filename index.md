## Description

Data scientists today search large data lakes to discover and integrate datasets. In order to bring together disparate data sources, dataset discovery methods rely on some form of schema matching: the process of establishing correspondences between datasets. Traditionally, schema matching has been used to find matching pairs of columns between a source and a target schema. However, the use of schema matching in dataset discovery methods differs from its original use. Nowadays schema matching serves as a building block for indicating and ranking inter-dataset relationships. Surprisingly, although a discovery method’s success relies highly on the quality of the underlying matching algorithms, the latest discovery methods employ existing schema matching algorithms in an ad-hoc fashion due to the lack of openly-available datasets with ground truth, reference method implementations, and evaluation metrics.

We aim to rectify the problem of evaluating the effectiveness and efficiency of schema matching methods for the specific needs of dataset discovery. To this end, we propose ***Valentine***, an extensible open-source experiment suite to execute and organize large-scale automated matching experiments on tabular data. Valentine includes implementations of seminal schema matching methods that we either implemented from scratch (due to absence of open source code) or imported from open repositories. The contributions of Valentine are: *i)* the definition of four schema matching scenarios as encountered in dataset discovery methods, *ii)* a principled dataset fabrication process tailored to the scope of dataset discovery methods and *iii)* the most comprehensive evaluation of schema matching techniques to date, offering insight on the strengths and weaknesses of existing techniques, that can serve as a guide for employing schema matching in future dataset discovery methods.

## Authors

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="https://ckoutras.github.io/"><img src="./assets/img/christos_koutras.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Christos Koutras <br>TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="https://www.tudelft.nl/ewi/over-de-faculteit/afdelingen/software-technology/web-information-systems/people/georgios-siachamis/"><img src="./assets/img/georgios_siachamis.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Georgios Siachamis <br>TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
   <a href="https://andraionescu.github.io/"><img src="./assets/img/andra_ionescu.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Andra Ionescu <br>TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <img src="./assets/img/kyriakos_psarakis.jpeg" height="auto" width="80" style="border-radius:50%"/>
    <figcaption class="caption" style="display:block">Kyriakos Psarakis <br>TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <img src="./assets/img/jerry_brons.jpg" height="auto" width="80" style="border-radius:50%"/>
    <figcaption class="caption" style="display:block">Jerry Brons <br>ING</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="http://mariosfragkoulis.gr/"><img src="./assets/img/marios_fragkoulis.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Marios Fragkoulis <br> TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="https://www.tudelft.nl/ewi/over-de-faculteit/afdelingen/software-technology/web-information-systems/people/christoph-lofi/"><img src="./assets/img/christoph_lofi.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Christoph Lofi <br>TU Delft</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="https://perso.liris.cnrs.fr/angela.bonifati/"><img src="./assets/img/angela_bonifati.jpg" height="auto" width="80" style="border-radius:50%"/></a>    <figcaption class="caption" style="display:block">Angela Bonifati <br>Lyon 1 University</figcaption>
</figure>

<figure class="item" style="vertical-align:top; display: inline-block; text-align:center; width:200px">
    <a href="http://asterios.katsifodimos.com/"><img src="./assets/img/asterios_katsifodimos.jpg" height="auto" width="80" style="border-radius:50%"/></a>
    <figcaption class="caption" style="display:block">Asterios Katsifodimos <br>TU Delft</figcaption>
</figure>

## Valentine Methods

The schema matching methods included in Valentine are the following:

1. **COMA**: Python wrapper around COMA 3.0 Community Edition.
2. **Cupid**: Contains the Python implementation of the paper *"Generic Schema Matching with Cupid"* (VLDB 2001).
3. **Distribution-based**: Contains the python implementation of the paper *"Automatic Discovery of Attributes in Relational Databases"* (SIGMOD 2011).
4. **EmbDI**: Contains the code of EmbDI provided by the authors in their GitLab repository and the paper *"Creating Embeddings of Heterogeneous Relational Datasets for Data Integration Tasks"* (SIGMOD 2020).
5. **Jaccard Levenshtein**: Contains our own baseline that uses Jaccard Similarity between columns to assess their correspondence score, enhanced by Levenshtein Distance.
6. **SemProp**: Contains the code of the method discussed in *"Seeping semantics: Linking datasets using word embeddings for data discovery"* (ICDE 2018), which is provided in the code repository of the paper *"Aurum: A Data Discovery System"* (ICDE 2018).
7. **Similarity Flooding**: Contains the python implementation of the paper *"Similarity Flooding: A Versatile Graph Matching Algorithm and its Application to Schema Matching"* (ICDE 2002).



## Datasets

Valentine offers a wide spectrum of dataset pairs with ground truth containing valid matches among theri corresponding columns. These dataset pairs have been fabricated by Valentines dataset relatedness scenario generator. In our paper, we classify relatedness of two datasets into the following four categories: i) *Unionable datasets*, ii) *View-Unionable datasets*, iii) *Joinable datasets*, and iv) *Semantically-Joinable datasets*.

In the table below, we specify the dataset sources and links to the corresponding fabricated dataset pairs, with respect to each relatedness scenario. We also specify min and max number of rows and columns of the fabricated datasets.

|Dataset Source | #Pairs |     #Rows     | #Columns |                            Links                           |
|----------------|:------:|:-------------:|:--------:|:----------------------------------------------------------:|
|[TPC-DI](http://www.vldb.org/pvldb/vol7/p1367-poess.pdf)         |   180  |  7492 - 14983 |  11 - 22 | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FView-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fprospect%2FSemantically-Joinable) |
|[Open Data](http://www.vldb.org/pvldb/vol11/p813-nargesian.pdf)      |   180  | 11628 - 23255 |  26 - 51 | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FView%20-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fmiller2%2FSemantically-Joinable) |
|[ChEMBL](https://www.ebi.ac.uk/chembl/)         |   180  |  7500 - 15000 |  12 - 23 | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FUnionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FView-Unionable), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FJoinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2Fassays%2FSemantically-Joinable) |
|[WikiData](https://www.wikidata.org)       |    4   |  5423 - 10846 |  13 - 20 | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_unionable), [View-Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_viewunion), [Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_joinable), [Semantically-Joinable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FWikidata%2FMusicians%2FMusicians_semjoinable) |
|[Magellan Data](https://sites.google.com/site/anhaidgroup/useful-stuff/data)| 7| 864 - 131099 | 3 - 7 | [Unionable](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku?path=%2FDeepMDatasets) |

### Filename Conventions

The filename conventions we use for the above datasets are explained as follows:

- *ac* and *ec* mean that the dataset pairs have noisy or verbatim schemata respectively.
- *av* and *ac* mean that the dataset pairs have noisy or verbatim instances.
- *horizontal_p* means that the datasets are derived from a horizontal split of p% row overlap based on the original dataset.
- *vertical_p* means that the datasets are derived from a vertical split of p% column overlap based on the original dataset.
- *both_p1\_p2* means that the datasets are derived from both a horizontal slit of p1% row overlap and a vertical split of p2% column overlap based on the original dataset.


## Repositories

- <https://github.com/delftdata/valentine> : Main repository containing the Valentine framework source code.
- <https://github.com/delftdata/valentine-generator> : Contains the source code of the dataset generator of Valentine.
- <https://github.com/delftdata/valentine-paper-results> : Contains detailed experimental results and plots based on the paper's evaluation.

## Valentine Paper
- [[arXiv](https://arxiv.org/abs/2010.07386)] Valentine: Evaluating Matching Techniques for Dataset Discovery 

## Cite Valentine

```
@misc{koutras2020valentine,
      title={Valentine: Evaluating Matching Techniques for Dataset Discovery}, 
      author={Christos Koutras and George Siachamis and Andra Ionescu and Kyriakos Psarakis and Jerry Brons and Marios Fragkoulis and Christoph Lofi and Angela Bonifati and Asterios Katsifodimos},
      year={2020},
      eprint={2010.07386},
      archivePrefix={arXiv},
      primaryClass={cs.DB}
}

```

