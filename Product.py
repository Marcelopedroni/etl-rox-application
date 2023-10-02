# -*- coding: utf-8 -*-
"""Module Import
Phase: Ingestion
Name: Product
"""

import csv_urls as url
import tables_dict as table_ref
import configs
import pandas as pd

# Initialize module parameter
table_product = table_ref.table_product

# set up bq object
client = configs.client

# delete previous version of table if exists
client.delete_table(table_product, not_found_ok=True)

# put original csv data into a DataFrame
df = pd.read_csv(url.product,";")

new = pd.DataFrame()
new['ProductID']             = df['ProductID'].astype('int')
new['Name']                  = df['Name'].str.strip().astype('string')
new['ProductNumber']         = df['ProductNumber'].str.strip().astype('string')
new['MakeFlag']              = df['MakeFlag'].astype('int')
new['FinishedGoodsFlag']     = df['FinishedGoodsFlag'].astype('int')
new['Color']                 = df['Color'].str.strip().astype('string')
new['SafetyStockLevel']      = df['SafetyStockLevel'].astype('int')
new['ReorderPoint']          = df['ReorderPoint'].astype('int')
new['StandardCost']          = df['StandardCost'].apply(lambda x: float(x.split()[0].replace(',', '')))
new['ListPrice']             = df['ListPrice'].apply(lambda x: float(x.split()[0].replace(',', '')))
new['Size']                  = df['Size'].str.strip().astype('string')
new['SizeUnitMeasureCode']   = df['SizeUnitMeasureCode'].str.strip().astype('string')
new['WeightUnitMeasureCode'] = df['WeightUnitMeasureCode'].str.strip().astype('string')
new['Weight']                = df['Weight'].astype('float')
new['DaysToManufacture']     = df['DaysToManufacture'].astype('int')
new['ProductLine']           = df['ProductLine'].str.strip().astype('string')
new['Class']                 = df['Class'].str.strip().astype('string')
new['Style']                 = df['Style'].str.strip().astype('string')
new['ProductSubcategoryID']  = df['ProductSubcategoryID'].fillna(0).astype('int')
new['ProductModelID']        = df['ProductModelID'].fillna(0).astype('int')
new['SellStartDate']         = pd.to_datetime(df['SellStartDate'])
new['SellEndDate']           = pd.to_datetime(df['SellEndDate'])
new['DiscontinuedDate']      = pd.to_datetime(df['DiscontinuedDate'])
new['rowguid']               = df['rowguid'].str.strip().astype('string')
new['ModifiedDate']          = pd.to_datetime(df['ModifiedDate'])

# load dataFrame into a BQ table
job = client.load_table_from_dataframe(new, table_product, job_config=configs.job_config)
job.result()

# show results
table = client.get_table(table_product)
print (f' "Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_product}"')
