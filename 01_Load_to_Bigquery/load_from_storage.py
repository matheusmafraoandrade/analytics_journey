#%% Packages
import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = os.getenv('GCP_BIGQUERY_SA')

#%% Functions
def list_buckets() -> list:    
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    tables = []
    for bucket in buckets:
        tables.append(bucket.name.split('analytics_journey_')[1])
        
    return tables

def load_from_storage(table_name: str) -> None:
    client = bigquery.Client()

    table_id = f"{client.project}.data_warehouse.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    
    bucket_name = f"analytics_journey_{table_name}"
    uri = f"gs://{bucket_name}/*.parquet"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows into {table_name}.")

#%% Load
tables = list_buckets()

for table_name in tables:
    if table_name == 'customers':
        continue
    load_from_storage(table_name)
