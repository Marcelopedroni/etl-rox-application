# -*- coding: utf-8 -*-
"""Module Import
Phase: Ingestion
Name: SalesCustomer
"""

import csv_urls as url
import tables_dict as table_ref
import configs
import pandas as pd

# Initialize module parameter
table_sales_customer = table_ref.table_sales_customer

# set up bq object
client = configs.client

# delete previous version of table if exists
client.delete_table(table_sales_customer, not_found_ok=True)

df = pd.read_csv(url.sales_customer,";")

new = pd.DataFrame()
new['CustomerID']    = df['CustomerID'].astype('int')
new['PersonID']      = df['PersonID'].fillna(0).astype('int')
new['StoreID']       = df['StoreID'].fillna(0).astype('int')
new['TerritoryID']   = df['TerritoryID'].astype('int')
new['AccountNumber'] = df['AccountNumber'].str.strip().astype('string')
new['rowguid']       = df['rowguid'].str.strip().astype('string')
new['ModifiedDate']  = pd.to_datetime(df['ModifiedDate'])

# load dataFrame into a BQ table
job = client.load_table_from_dataframe(new, table_sales_customer, job_config=configs.job_config)
job.result()

# show results
table = client.get_table(table_sales_customer)
print (f' "Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_sales_customer}"')
