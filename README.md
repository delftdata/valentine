# Schema matching tools
Experimentation with existing schema matching methods

The repository contains the following:
1. Module _clustering_ with the implementation of 
[Automatic Discovery of attributes in relational database](https://www.researchgate.net/profile/Divesh_Srivastava2/publication/221213724_Automatic_Discovery_of_Attributes_in_Relational_Databases/links/55edd50108ae0af8ee19d399/Automatic-Discovery-of-Attributes-in-Relational-Databases.pdf)
2. Folder _seeping-semantics_ with scripts and guidelines on how to set up
[aurum-datadiscovery](https://github.com/mitdbg/aurum-datadiscovery). It contains the implementation
of [Seeping Semantics: Linking Datasets using Word Embeddings for Data Discovery](https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=8509314)
3. Module _cupid_ with the implementation of [Generic Schema Matching with Cupid](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2001-58.pdf)
4. Folder with draft jupiter notebooks. 
3. Example scripts to read data for clustering and how to run it.
4. Requirements files for each module. 

## 1. Clustering 

### Before running
Before running CorrelationClustering, create a
virtual environment and install the requirements from 
*requirements-clustering.txt*

Prerequisites: Python3.6 or Python3.7

### File descriptions

#### column_model
The file contains the data model used in the application. 
The "Column" is a representation of a column from a database. 
To map a dataset into _Columns_, read each column of the dataset
and create a new _Column_ for each by specifying the name of the column,
the data and the source (table name). 

Next, _process_data_ is required in order to process the entire dataset
before performing any other operations. 

All of the above operations are automatically done by the main class
of the module _CorrelationClustering_

#### emd_utils
The file contains the functions that computes different kinds of
Earth Mover's Distance (EMD) algorithms. 

#### discovery
The file contains the main algorithms described in the paper.

#### correlation_clustering
The file contains the main class of the application. 
To instantiate and work with it, the following steps must be followed:
1. Create a new _CorrelationClustering_ object and specify 
the number of quantiles used to compute histograms and
the threshold used to compare EMD values (start small e.g. 0.05)
2. Add data using _add_data()_. The method requires a *Pandas DataFrame*
object, the source name (table name) and what columns should be used for 
matchings (optional parameter, the default uses all the columns).
3. Process the data (_process_data()_). The method will make the
required transformation in order to convert the data in _Column_ format.
4. Find matchings (_find_matchings()_). This might take a while,
since it is the main function of the application. The result is a list of 
mappings. 

### Test
Outside the module you can find 
_read_data_movies_ and _read_data_tpch_ which are example files of reading data.

*test_clustering* contains the steps to test the module. 


## 2. seeping-semantics
The folder contains its own README that will help with running the application.

## 3. Cupid 
### Before running
Before running the cupid pipeline, create a virtual environment and install the requirements
from requirements-cupid.txt

Prerequisites: Python3.6 or Python3.7

### 

