import pandas as pd

rows = None
customer = pd.read_csv('../data/tpch/customer.csv', sep='|', nrows=rows)
# lineitem = pd.read_csv('tpch/lineitem.csv', sep='|', nrows=rows)
nation = pd.read_csv('../data/tpch/nation.csv', sep='|', nrows=rows)
orders = pd.read_csv('../data/tpch/orders.csv', sep='|', nrows=rows)
# part = pd.read_csv('tpch/part.csv', sep='|', nrows=rows)
# region = pd.read_csv('tpch/region.csv', sep='|', nrows=rows)
# supplier = pd.read_csv('tpch/supplier.csv', sep='|', nrows=rows)
# partsupp = pd.read_csv('tpch/partsupp.csv', sep='|', nrows=rows)

small_cust = customer[['C_CustKey', 'C_Name', 'C_Address', 'C_NationKey', 'C_Phone', 'C_Comment']].copy()
small_order = orders[['O_OrderKey', 'O_CustKey', 'O_OrderPriority', 'O_Comment']].copy()

asian_n = nation.loc[nation['N_RegionKey'] == 2]
asian_customer = customer.loc[customer['C_NationKey'].isin(asian_n['N_NationKey'])].copy()
asian_customer = asian_customer[['C_CustKey', 'C_Name', 'C_Address', 'C_NationKey', 'C_Phone', 'C_Comment']]
asian_customer.rename(columns=lambda x: 'A'+x, inplace=True)

eur_n = nation.loc[nation['N_RegionKey'] == 3]
eur_customer = customer.loc[customer['C_NationKey'].isin(eur_n['N_NationKey'])].copy()
eur_customer = eur_customer[['C_CustKey', 'C_Name', 'C_Address', 'C_NationKey', 'C_Phone', 'C_Comment']]
eur_customer.rename(columns=lambda x: 'E'+x, inplace=True)

data = pd.concat([small_cust, nation, small_order, asian_customer, eur_customer], axis=1)
data = data.fillna(0)

print(data.columns)



