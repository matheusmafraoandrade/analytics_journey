#%% Packages
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = os.getenv('GCP_BIGQUERY_SA')

#%% Function
# Construct a BigQuery client object.
client = bigquery.Client()

dataset_id = "{}.data_warehouse".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)
dataset.location = "us-east1"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))