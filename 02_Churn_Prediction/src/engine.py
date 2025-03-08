# %% 
# Packages
import os
import polars as pl
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GCP_BIGQUERY_SA')

# %%
# Functions
def run_query(file_name: str) -> pl.DataFrame:
    
    with open(f'etl/{file_name}.sql', 'r') as f:
        query = f.read()

    client = bigquery.Client()

    # Perform a query.
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    
    return df

# %%
# Run
df = run_query('pagamentos')
df

# %%
