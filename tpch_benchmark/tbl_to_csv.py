import pandas as pd


def transform_tbl_to_csv(tbl_file_path: str, dataset_path: str):

    df = pd.read_csv(tbl_file_path+'customer.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                              'c_comment'])

    df.to_csv(dataset_path+'customer.csv', index=False)

    df = pd.read_csv(tbl_file_path+'lineitem.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                              'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                              'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'])

    df.to_csv(dataset_path+'lineitem.csv', index=False)

    df = pd.read_csv(tbl_file_path+'nation.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'])

    df.to_csv(dataset_path+'nation.csv', index=False)

    df = pd.read_csv(tbl_file_path+'orders.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                              'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'])

    df.to_csv(dataset_path+'orders.csv', index=False)

    df = pd.read_csv(tbl_file_path+'part.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container',
                              'p_retailprice', 'p_comment'])

    df.to_csv(dataset_path+'part.csv', index=False)

    df = pd.read_csv(tbl_file_path+'partsupp.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'])

    df.to_csv(dataset_path+'partsupp.csv', index=False)

    df = pd.read_csv(tbl_file_path+'region.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['r_regionkey', 'r_name', 'r_comment'])

    df.to_csv(dataset_path+'region.csv', index=False)

    df = pd.read_csv(tbl_file_path+'supplier.tbl',
                       delimiter='|',
                       header=None,
                       index_col=False,
                       names=['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'])

    df.to_csv(dataset_path+'supplier.csv', index=False)


if __name__ == "__main__":

    tbl_fp = "../data/TPCH_base_tables_scale1/"
    dataset_fp = "../data/TPCH_base_tables_scale1/"

    transform_tbl_to_csv(tbl_fp, dataset_fp)
