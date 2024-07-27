#%%
# Monitor upload progress 
from tqdm import tqdm

# Deal with dataframes and formats
import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

# Connect to Google Cloud Storage
from google.cloud import storage

# Configure environment
import os
from dotenv import load_dotenv

# It is important to name the credentials as GOOGLE_APPLICATION_CREDENTIALS for the Storage package to work accordingly 
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = os.getenv('GCP_CLOUD_STORAGE_SA')

#%%
# Function that creates the bucket
def create_bucket(bucket_name: str,
                  storage_class: str = 'STANDARD',
                  location: str = 'us-east1') -> str: 
    
    storage_client = storage.Client()

    bucket = storage_client.bucket(f"analytics_journey_{bucket_name}")
    bucket.storage_class = storage_class
   
    bucket = storage_client.create_bucket(bucket, location=location)
    
    return print(f'Bucket {bucket.name} created.')

# Function that uploads a file to the bucket
def upload_files(bucket_name: str,
                 partition_col: str = None) -> str:
    
    # Read the CSV file
    path = fr"data\ERP\{bucket_name}.csv"
    csv_data = pd.read_csv(path)
    
    # Convert pandas dataframe to pyarrow table
    table = pa.Table.from_pandas(csv_data)
    
    # Instantiate Cloud Storage file system
    gcs = fs.GcsFileSystem()
    
    # Write Arrow table to Parquet files
    if partition_col:
        uri = fr"analytics_journey_{bucket_name}/partitioned_data/"
        
        # Files partitioned by state
        if "state" in partition_col:
            pq.write_to_dataset(
                table,
                root_path=uri,
                filesystem=gcs,
                partition_cols=[partition_col]
            )
        # Files partitioned by date
        else:
            csv_data['reference_date'] = pd.to_datetime(csv_data[partition_col]).dt.date
            pq.write_to_dataset(
                table,
                root_path=uri,
                filesystem=gcs,
                partition_cols=['reference_date']
            )
    # Files without partition
    else:
        uri = fr"analytics_journey_{bucket_name}/{bucket_name}.parquet"
        
        pq.write_table(
            table,
            uri,
            filesystem=gcs
        )
    
    return print(f"Parquet files {bucket_name} created!")
        

#%%
files_dir = os.listdir(r'data\ERP')

# Create a bucket for each file and load converted Parquet files
for file in tqdm(files_dir, total=len(files_dir), desc="GCS upload progress"):
    bucket_name = file.split('.')[0]
    
    # Define the partition column for each file
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
            partition_col = None # No partition for other files

    create_bucket(bucket_name)
    upload_files(bucket_name, partition_col)
    
    print("--------------------------")

print("Uploaded files successfully!")

# %%
