# -*- coding: utf-8 -*-    
# Get BigQuery Table

import pandas as pd
from google.cloud import bigquery

def read_bq_table(table_id: str, client=bigquery.Client()):
    """
    Reads a Google BigQuery table into a Pandas DataFrame.

    Args:
        table_id (str): The full ID of the table in "your-gcp-project-id.your_dataset_name.your_table_name" format.
    """
    # 1. Create a BigQuery client
    bq_client = client

    # 2. Construct the query to select all data from the table
    query = f"SELECT * FROM `{table_id}`"

    # 3. Run the query and convert the job result to a DataFrame
    df = bq_client.query(query).to_dataframe()

    return df