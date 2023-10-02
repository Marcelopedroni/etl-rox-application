### This is the ETL processing for Rox Application ###

![Project Architecture](image.png)

# Architecture #
On this project the architecture choosen is  all based on Google Cloud environment (GCP). 
The data flow is:
  1. Create Google Project: ETL-Rox-application
  2. Enable BigQuery API
  3. Create buckets to hold csv's: There is a main directory called "application_data_base" and inside of it there are 3 "folders": 
     - person_data (for Person.Person.csv);
     - production_data (for Production.Product.csv)
     - sales_data (for Sales.Customer.csv, Sales.SalesOrderDetail, Sales.SalesOrderHeader and Sales.SalesSpecialOfferProduct)
  4. Upload all 6 csv's files into right folders using the following gcp command lines:
    - $ gsutil cp Production.Product.csv gs://application_data_base/production_data
    - $ gsutil cp Person.Person.csv gs://application_data_base/person_data
    - $ gsutil cp Sales.Customer.csv Sales.SalesOrderDetail.csv Sales.SalesOrderHeader.csv Sales.SpecialOfferProduct.csv gs://application_data_base/sales_data
  5. You're good to go foward on running the modules

# Cloud Ingestion #
It performs cloud ingestion operations for 6 files that needs to be uploaded manually on the right folders such as descript on the csv_urls.py file

# Running the modules #
### STEPS ###
1. Install Python (3.8.10 or higher) and install pip. (Available on: https://www.python.org/downloads/)
2. Install Google Cloud SDK and log in into the project (Available on: https://cloud.google.com/sdk/docs/install)
3. Install all project dependencies by running $ pip -r requirements.txt.
4. Just run $ python <module.py>

INFO: All tables are re-created on each execution.