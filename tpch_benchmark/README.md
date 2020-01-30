## Instructions 

### Download the variation of the TPC-H dataset as defined in [1]
1. [Install gsutil](https://cloud.google.com/storage/docs/gsutil_install) to access the csv 
formatted TPC-H dataset together with the views from [1]
2. Run `gsutil -m cp -r gs://schema-matching-tools-tpch/TPCH [SAVE_TO_LOCATION]` which stores the aforementioned 
 dataset to the `[SAVE_TO_LOCATION]` path that you define.

### Reproduce the creation of the TPC-H 
1. Download the TPC-H dbgen tools from [here](http://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=2.18.0&mode=CURRENT-ONLY)
2. Create a MAKEFILE based on the instructions on dbgen's `makefile.suite` 
3. Run `make`
4. Run dbgen with the desired parameters (`-s` is the scale factor and it defines the approximate size of the dataset in GB)
5. Take the 8 generated base tables in `.tbl` format and pass them through the script `tbl_to_csv.py` to transform them to csv files (remember to change the paths to yours)
6. Run the `create_views.py` to create the 110 views (remember to change the paths to yours)


### Gold standard
The gold standard rules exist in the `tpch_gold_standard.py` script


* [1] Automatic Discovery of Attributes in Relational Databases M. Zhang et al.