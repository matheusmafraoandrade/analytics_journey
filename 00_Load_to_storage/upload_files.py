#%% Packages
import os
from tqdm import tqdm

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = os.getenv('GCP_CLOUD_STORAGE_SA')

#%% Functions
# define function that creates the bucket
def create_bucket(bucket_name: str,
                  storage_class: str = 'STANDARD',
                  location: str = 'us-east1') -> str: 
    
    storage_client = storage.Client()

    bucket = storage_client.bucket(f"analytics_journey_{bucket_name}")
    bucket.storage_class = storage_class
   
    bucket = storage_client.create_bucket(bucket, location=location)
    
    return print(f'Bucket {bucket.name} created.')

# define function that uploads a file from the bucket
def upload_files(bucket_name: str,
                 partition_col: str = None) -> str:
    
    # Read the CSV file
    path = fr"data\ERP\{bucket_name}.csv"
    csv_data = pd.read_csv(path)
    table = pa.Table.from_pandas(csv_data)
    
    # Cloud Storage file system
    gcs = fs.GcsFileSystem()
    
    if partition_col:
        uri = fr"analytics_journey_{bucket_name}/partitioned_data/"
        
        if "state" in partition_col:
            pq.write_to_dataset(
                table,
                root_path=uri,
                filesystem=gcs,
                partition_cols=[partition_col]
            )
        else:
            csv_data['reference_date'] = pd.to_datetime(csv_data[partition_col]).dt.date
            pq.write_to_dataset(
                table,
                root_path=uri,
                filesystem=gcs,
                partition_cols=['reference_date']
            )
    else:
        uri = fr"analytics_journey_{bucket_name}/{bucket_name}.parquet"
        
        pq.write_table(
            table,
            uri,
            filesystem=gcs
        )
    
    return print(f"Parquet files {bucket_name} created!")
        

#%% Load
files_dir = os.listdir(r'data\ERP')

for file in tqdm(files_dir, total=len(files_dir), desc="GCS upload progress"):
    bucket_name = file.split('.')[0]
    
    match bucket_name:
        case 'geolocation':
            partition_col = 'geolocation_state'
        case 'order_items':
            partition_col = 'shipping_limit_date'
        case 'order_reviews':
            partition_col = 'review_creation_date'
        case 'orders':
            partition_col = 'order_purchase_timestamp'
        case _:
            partition_col = None

    create_bucket(bucket_name)
    upload_files(bucket_name, partition_col)
    
    print("--------------------------")

print("Uploaded files successfully!")

# %%
