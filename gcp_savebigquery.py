# -*- coding: utf-8 -*-    
# Save BigQuery Table
import pandas as pd
from google.cloud import bigquery

def save_to_bq(df: pd.DataFrame, table_id: str, client=bigquery.Client(), write_disposition: str = 'WRITE_TRUNCATE'):
    """
    Saves a Pandas DataFrame to a Google BigQuery table.

    If the table does not exist, it will be created.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_id (str): The destination table ID in "your-gcp-project-id.your_dataset_name.your_table_name" format.
        write_disposition (str, optional): Specifies the write action.
            'WRITE_TRUNCATE' to overwrite the table.
            'WRITE_APPEND' to append to the table.
            Defaults to 'WRITE_TRUNCATE'.
    """
    # 1. Create a BigQuery client
    bq_client = client

    # 2. Configure the load job to specify the write disposition and schema detection
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        # Let BigQuery create the schema from the DataFrame's dtypes
        autodetect=True,
    )

    # 3. Start the load job and wait for it to complete
    load_job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    load_job.result()  # This line waits for the job to finish

    # 4. Print a confirmation message
    destination_table = bq_client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows into table {table_id}")