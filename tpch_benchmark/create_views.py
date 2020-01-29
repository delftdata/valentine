import pandas as pd
import os

dataset_store_path = "../data/zero_two/"  # Change this to the path of the base tables


def create_views(fn: str, column_list: list, folder: str):
    create_directory(folder)
    df = pd.read_csv(dataset_store_path + fn)

    for column in column_list:
        if column == 'c_acctbal':
            positive = df[df[column] >= 0].loc[:, ~df.columns.isin(['c_acctbal'])]
            negative = df[df[column] < 0].loc[:, ~df.columns.isin(['c_acctbal'])]

            positive.to_csv(folder+'p_acctbal_'+fn, index=False)
            negative.to_csv(folder+'n_acctbal_'+fn, index=False)

        else:
            if column not in ['c_nationkey', 's_nationkey', 'n_regionkey']:
                unique_elements = set(df[column])
                for seg in unique_elements:
                    seg_data = df[df[column] == seg]

                    seg_data.to_csv(folder+seg.lower()+'_'+fn, index=False)

            elif column == 'n_regionkey':

                region = pd.read_csv(dataset_store_path + "region.csv")

                region_columns = list(region.columns)

                joined_data_nation_region = pd.merge(df, region, left_on="n_regionkey",
                                                     right_on='r_regionkey', how='inner')

                unique_elements_region = set(joined_data_nation_region['r_name'])

                for seg in unique_elements_region:
                    seg_data = joined_data_nation_region[joined_data_nation_region['r_name'] == seg]\
                                .loc[:, ~joined_data_nation_region.columns.isin(region_columns)]

                    seg_data.to_csv(folder + seg.lower() + '_' + fn, index=False)

            else:
                nation = pd.read_csv(dataset_store_path + "nation.csv")
                region = pd.read_csv(dataset_store_path + "region.csv")

                region_columns = list(region.columns)
                nation_columns = list(nation.columns)

                joined_data_nation = pd.merge(df, nation, left_on=column, right_on='n_nationkey', how='inner')
                joined_data_nation_region = pd.merge(joined_data_nation, region, left_on="n_regionkey",
                                                     right_on='r_regionkey', how='inner')

                unique_elements_nation = set(joined_data_nation_region['n_name'])
                unique_elements_region = set(joined_data_nation_region['r_name'])

                for seg in unique_elements_nation:
                    seg_data = joined_data_nation_region[joined_data_nation_region['n_name'] == seg]\
                            .loc[:, ~joined_data_nation_region.columns.isin(region_columns+nation_columns)]

                    # if column == 'c_nationkey':

                    seg_data.to_csv(folder + seg.lower() + '_' + fn, index=False)

                for seg in unique_elements_region:
                    seg_data = joined_data_nation_region[joined_data_nation_region['r_name'] == seg]\
                                .loc[:, ~joined_data_nation_region.columns.isin(region_columns+nation_columns)]

                    seg_data.to_csv(folder + seg.lower() + '_' + fn, index=False)


def size_of_dataset(path):
    directory = os.path.join(path)
    c_count = 0
    r_count = 0
    v_count = -8
    for root, dirs, files in os.walk(directory):
        for file in files:
            df = pd.read_csv(root+"/"+file)
            c_count = c_count + len(df.columns)
            r_count = r_count + len(df.index)
            v_count = v_count + 1
    return c_count, r_count, v_count


def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == "__main__":
    create_views("customer.csv", ['c_acctbal', 'c_mktsegment', 'c_nationkey'], dataset_store_path + 'customer_views/')
    create_views("nation.csv", ['n_regionkey'], dataset_store_path + 'nation_views/')
    create_views("part.csv", ['p_brand', 'p_mfgr'], dataset_store_path + 'part_views/')
    create_views("supplier.csv", ['s_nationkey'], dataset_store_path + 'supplier_views/')
    create_views("orders.csv", ['o_orderstatus', 'o_orderpriority'], dataset_store_path + 'orders_views/')

    column_count, row_count, view_count = size_of_dataset(dataset_store_path)

    print("Number of views: ", view_count)
    print("Number of columns: ", column_count)
    print("Number of rows: ", f'{row_count:,}')
