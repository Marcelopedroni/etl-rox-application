# -*- coding: utf-8 -*-
"""Module Import
Phase: Ingestion
Name: SalesOrderDetail
"""

import csv_urls as url
import tables_dict as table_ref
import configs
import pandas as pd

# Initialize module parameter
table_sales_special_offer_product = table_ref.table_sales_special_offer_product

# set up bq object
client = configs.client

# delete previous version of table if exists
client.delete_table(table_sales_special_offer_product, not_found_ok=True)

# put original csv data into a DataFrame
df = pd.read_csv(url.sales_special_offer_product,";")

new = pd.DataFrame()
new['SpecialOfferID'] = df['SpecialOfferID'].astype('int')
new['ProductID']      = df['ProductID'].astype('int')
new['rowguid']        = df['rowguid'].str.strip().astype('string')
new['ModifiedDate']   = pd.to_datetime(df['ModifiedDate'])

# load dataFrame into a BQ table
job = client.load_table_from_dataframe(new, table_sales_special_offer_product, job_config=configs.job_config)
job.result()

# show results
table = client.get_table(table_sales_special_offer_product)
print (f' "Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_sales_special_offer_product}"')
