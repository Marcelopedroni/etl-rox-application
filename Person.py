# -*- coding: utf-8 -*-
"""Module Import
Phase: Ingestion
Name: Person
"""

import csv_urls as url
import tables_dict as table_ref
import configs
import pandas as pd

# Initialize module parameters
table_person = table_ref.table_person

# set up bq object
client = configs.client

# delete previous version of table if exists
client.delete_table(table_person, not_found_ok=True)

# put original csv data into a DataFrame
df = pd.read_csv(url.person,";")

new = pd.DataFrame()
new['BusinessEntityID']      = df['BusinessEntityID'].astype('int')
new['PersonType']            = df['PersonType'].str.strip()
new['NameStyle']             = df['NameStyle'].astype('int')
new['Title']                 = df['Title'].str.strip()
new['FirstName']             = df['FirstName'].str.strip()
new['MiddleName']            = df['MiddleName'].str.strip()
new['LastName']              = df['LastName'].str.strip()
new['Suffix']                = df['Suffix'].str.strip()
new['EmailPromotion']        = df['EmailPromotion'].astype('int')
new['AdditionalContactInfo'] = df['AdditionalContactInfo'].str.strip().replace('"','')
new['Demographics']          = df['Demographics'].str.strip().replace('"','')
new['rowguid']               = df['rowguid'].str.strip().replace('"','')
new['ModifiedDate']          = pd.to_datetime(df['ModifiedDate'])

new = new.fillna('')

# load dataFrame into a BQ table
job = client.load_table_from_dataframe(new, table_person, job_config=configs.job_config)
job.result()

# show results
table = client.get_table(table_person)
print (f' "Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_person}"')
