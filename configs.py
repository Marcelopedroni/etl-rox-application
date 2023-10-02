# set common job_config for all csv's ingestions
from google.cloud import bigquery

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True
)

client = bigquery.Client(project="etl-rox-application")
