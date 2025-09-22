# -*- coding: utf-8 -*-    
    
import pandas as pd
from google.cloud import storage
import io

def get_bucket_csv(bucket_name: str, blob_name: str, dtype_spec=None, date_cols=None, client = storage.Client()):
    """Reads a CSV file from Google Cloud Storage into a Pandas DataFrame. You must assign the bucket and blob name"""
    storage_client = client
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the blob content as bytes
    data = blob.download_as_bytes()

    # Use io.BytesIO to treat the bytes data as a file-like object
    df = pd.read_csv(io.BytesIO(data), dtype=dtype_spec, parse_dates=date_cols)
    return df

# Example usage:
    bucket_name = "your-gcs-bucket-name"
    blob_name = "path/to/your/file.csv"
    
    df = read_csv_from_gcs(bucket_name, blob_name)
    print(df.head())