#%% Imports
from typing import List
import dlt

from hubspot import hubspot, hubspot_events_for_objects, THubspotObjectType

import os
from dotenv import load_dotenv

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = os.getenv('GCP_BIGQUERY_SA')

#%% Functions
def load_crm_data() -> None:
    """
    This function loads all resources from HubSpot CRM

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="data_warehouse",
        destination='bigquery',
    )
    load_data = hubspot()

    # To read only particular fields:
    contact_props = [
        "firstname",
        "start_date",
        "landing_page_id",
        "origin"
    ]
    deal_props = [
        "dealname",
        "nome_do_lead",
        "lead_behavior_profile",
        "lead_type",
        "sales_representative",
        "sdr",
        "won_date"
    ]
    
    load_data.contacts.bind(props=contact_props, include_custom_props=False)
    load_data.deals.bind(props=deal_props, include_custom_props=False)
    
    # Run the pipeline with the HubSpot source connector
    info = pipeline.run(load_data.with_resources("contacts","deals"))

    # Print information about the pipeline run
    print(info)

#%% Load data
load_crm_data()