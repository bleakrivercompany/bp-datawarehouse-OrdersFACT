# -*- coding: utf-8 -*-
# gcp_savebucket

import pandas as pd
from google.cloud import storage

def save_bucket(df, bucket_name: str, blob_name: str, client = storage.Client()):
    """
    Saves a Pandas DataFrame as a CSV file to Google Cloud Storage.

    Args:
        df (pd.DataFrame): The Pandas DataFrame to save.
        bucket_name (str): The name of your GCS bucket.
        blob_name (str): The desired path and filename within the bucket (e.g., 'data/output.csv').
    """
    storage_client = client
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Convert DataFrame to a CSV string in memory
    csv_string = df.to_csv(index=False)  # index=False avoids writing the DataFrame index

    # Upload the CSV string to GCS, specifying content type
    blob.upload_from_string(csv_string, content_type='text/csv')
    print(f"DataFrame successfully saved to gs://{bucket_name}/{blob_name}")