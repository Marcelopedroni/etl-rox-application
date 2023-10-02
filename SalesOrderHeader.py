# -*- coding: utf-8 -*-
"""Module Import
Phase: Ingestion
Name: SalesOrderHeader
"""

import csv_urls as url
import tables_dict as table_ref
import configs
import pandas as pd

# Initialize module parameter
table_sales_order_header = table_ref.table_sales_order_header

# set up bq object
client = configs.client

# delete previous version of table if exists
client.delete_table(table_sales_order_header, not_found_ok=True)

# put original csv data into a DataFrame
df = pd.read_csv(url.sales_order_header,";")

new = pd.DataFrame()
new['SalesOrderID']           = df['SalesOrderID'].astype('int')
new['RevisionNumber']         = df['RevisionNumber'].astype('int')
new['OrderDate']              = pd.to_datetime(df['OrderDate'])
new['DueDate']                = pd.to_datetime(df['DueDate'])
new['ShipDate']               = pd.to_datetime(df['ShipDate'])
new['Status']                 = df['Status'].astype('int')
new['OnlineOrderFlag']        = df['OnlineOrderFlag'].astype('int')
new['SalesOrderNumber']       = df['SalesOrderNumber'].astype('string')
new['PurchaseOrderNumber']    = df['PurchaseOrderNumber'].astype('string')
new['AccountNumber']          = df['AccountNumber'].astype('string')
new['CustomerID']             = df['CustomerID'].astype('int')
new['SalesPersonID']          = df['SalesPersonID'].fillna(0).astype('int')
new['TerritoryID']            = df['TerritoryID'].astype('int')
new['BillToAddressID']        = df['BillToAddressID'].astype('int')
new['ShipToAddressID']        = df['ShipToAddressID'].astype('int')
new['ShipMethodID']           = df['ShipMethodID'].astype('int')
new['CreditCardID']           = df['CreditCardID'].fillna(0).astype('int')
new['CreditCardApprovalCode'] = df['CreditCardApprovalCode'].astype('string')
new['CurrencyRateID']         = df['CurrencyRateID'].fillna(0).astype('int')
new['SubTotal']               = df['SubTotal'].apply(lambda x: float(x.split()[0].replace(',', '.')))
new['TaxAmt']                 = df['TaxAmt'].apply(lambda x: float(x.split()[0].replace(',', '.')))
new['Freight']                = df['Freight'].apply(lambda x: float(x.split()[0].replace(',', '.')))
new['TotalDue']               = df['TotalDue'].apply(lambda x: float(x.split()[0].replace(',', '.')))
new['Comment']                = df['Comment'].astype('string')
new['rowguid']                = df['rowguid'].str.strip().astype('string')
new['ModifiedDate']           = pd.to_datetime(df['ModifiedDate'])

# load dataFrame into a BQ table
job = client.load_table_from_dataframe(new, table_sales_order_header, job_config=configs.job_config)
job.result()

# show results
table = client.get_table(table_sales_order_header)
print (f' "Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_sales_order_header}"')
