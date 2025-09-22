# -*- coding: utf-8 -*-
# MERGE INCREMENTS TO WC_ORDERS_COMPLETE AND SCB_COMPLETE

import pandas as pd
from datetime import date
from datetime import datetime
import numpy as np
import json
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
# Custom Functions
from gcp_getsecrets import get_gcp_secret
from gcp_postbucket import save_bucket
from gcp_getbucket import get_bucket_csv

# Define project
project_id = f"button-datawarehouse"
# Define storage bucket for push
bucket_name = "cs-royalties-test"  # Replace with your bucket name
# Define secrets to fetch
p_consumerkey = get_gcp_secret(project_id, "wc_consumer_key", "latest")
p_consumersecret = get_gcp_secret(project_id, "wc_consumer_secret", "latest")
secret_id_for_sa_key = "storage_sa_key" # The secret you just created
# get those secrets
sa_key_json_string = get_gcp_secret(project_id, secret_id_for_sa_key)
credentials_info = json.loads(sa_key_json_string)
credentials = service_account.Credentials.from_service_account_info(credentials_info)
storage_client = storage.Client(credentials=credentials, project=project_id)

# 2. Create a dictionary with your desired data types
# This is useful for IDs, ZIP codes, or columns with mixed types.
wc_dtypes = {'Order Number' : int ,
             'Order Status': object,
             'Total products' : int,
             'Total items' : int,
             'Order Subtotal Amount' : float,
             'Order Shipping Amount' : float,
             'Order Total Amount' : float,
             'Customer User ID' : object,
             'Customer Username' : object,
             'Customer User Email' : object,
             'Customer Role' : object,
             'City, State, Zip (Shipping)' : object,
             'Country Name (Shipping)' : object,
             'Bundle ID' : object,
             'Bundled By' : object,
             'Composite ID' : object,
             'Component of' : object,
             'ItemOrderSeq' : int,
             'Product Name' : object,
             'Quantity' : int,
             'Order Line Subtotal' : float,
             'Item Discount Amount' : float,
             'Order Line Total' : float,
             'Category' : object,
             'Subcategory' : object,
             'Gift Wrap' : object,
             'Gift Wrap Cost' : float,
             'Linked Author' : object,
             'Coupon Code' : object,
             'Coupon Type' : object,
             'Coupon Amount' : float,
             'Discount Amount' : float,
             'ShippingCity' : object,
             'ShippingState' : object,
             'ShippingZip' : object
             }

scb_dtypes = {'Month' : object,
              'Year' : int,
              'Title' : object,
              'ISBN' : object,
              'Quantity Shipped' : int,
              'Publisher Payment' : float,
              'Quantity Returned' : int,
              'Publisher Credits' : float,
              'Beginning Inventory' : int,
              'Quantity Received' : int,
              'Quantity Adjusted' : int,
              'Ending Inventory' : int,
              'MonthYear' : object
              }
#WooCom Increment
wci_blob_name = "stage/woocom_stage/WooCom_Increment_Stage.csv"
wc_i = get_bucket_csv(bucket_name, wci_blob_name, dtype_spec=wc_dtypes, date_cols=['Order Date']).reset_index(drop=True).drop_duplicates()
wc_i['Order Date'] = pd.to_datetime(wc_i['Order Date']).dt.date
# WooCom All Orders Archive
wcall_blob_name = "stage/woocom_stage/WooCom_Orders_Complete.csv"
wcall = get_bucket_csv(bucket_name, wcall_blob_name, dtype_spec=wc_dtypes, date_cols=['Order Date']).reset_index(drop=True).drop_duplicates()
# Date formatting
wcall['Order Date'] = pd.to_datetime(wcall['Order Date']).dt.date
wc_i['Order Date'] = pd.to_datetime(wc_i['Order Date']).dt.date
# WC Merch and Bundle are created before any changes get made
wc_merch = wcall[wcall[r'Category'] == 'Merch'].reset_index()
wc_bundle = wcall[wcall[r'Category'] == 'Bundles'].reset_index()

# Wholesale check
wcall.loc[wcall['Coupon Code'].isin(['WHOLESALE60', 'wholesale60', 'WHOLESALE40', 'wholesale40', 'WHOLESALE50', 'wholesale50']), 'Customer Role'] = 'Wholesale Customer'

#SCB Data
# Increment
scbi_blob_name = "stage/scb_stage/SCB_Increment_Stage.csv"
scb_i = get_bucket_csv(bucket_name, scbi_blob_name, dtype_spec=scb_dtypes).reset_index(drop=True)
# SCB Complete
scb_blob_name = "stage/scb_stage/SCB_Complete.csv"
scb = get_bucket_csv(bucket_name, scb_blob_name, dtype_spec=scb_dtypes).reset_index(drop=True)

# WooCom Merge
wcall['OrderItemIndex'] = wcall['Order Number'].astype(str) + wcall['ItemOrderSeq'].astype(str)
wcall = wcall.drop_duplicates(subset=['OrderItemIndex'])
wc_i['OrderItemIndex'] = wc_i['Order Number'].astype(str) + wc_i['ItemOrderSeq'].astype(str)
wc_i = wc_i.drop_duplicates(subset=['OrderItemIndex'])
wc_filter = ~wcall['Order Number'].isin(wc_i['Order Number'])
wc_2 = wcall[wc_filter].reset_index(drop=True)
wc_merge = pd.concat([wc_2, wc_i], ignore_index=True)
wc_merge2 = wc_merge.drop_duplicates()
wc_merge2 = wc_merge2.drop_duplicates(ignore_index=True).reset_index(drop=True)
# Save to Stage
save_bucket(wc_merge2, bucket_name, 'stage/woocom_stage/WooCom_Orders_Complete.csv')

#SCB Merge
scb_filter = ~scb['MonthYear'].isin(scb_i['MonthYear'])
scb_2 = scb[scb_filter].reset_index(drop=True)
scb_merge = pd.concat([scb_2, scb_i], ignore_index=True).reset_index(drop=True)
scb_merge = scb_merge.drop_duplicates()
# Save to Stage
save_bucket(scb_merge, bucket_name, 'stage/scb_stage/SCB_Complete.csv')

# Create and save WooCom and SCB book dimensions for quicker processing in next ETL stage
"""
WOOCOM BOOK CLEAN AND SAVE

BASIC CONCEPT HERE IS TO CREATE CLEAN COLUMNS WE CAN MERGE TO
APPLY SAME CLEANING LOGIC TO SOURCE SYSTEM PULLS
"""
wc_group = wc_merge2.groupby(['Product Name','Category']).size().reset_index().drop_duplicates()
wc_group = wc_group.filter(['Product Name', 'Category'], axis=1)

wc_book = wc_group[wc_group[r'Category'] == 'Book'].copy()

# 1. Split 'Product Name' into 'Title' and 'TypeString'
wc_book[['Title', 'TypeString']] = wc_book['Product Name'].str.split(' - ', n=1, expand=True)

# 2. Fill missing TypeString values by reassigning the column (removes inplace=True)
wc_book['TypeString'] = wc_book['TypeString'].fillna("Print")

# 3. Use np.select() for efficient conditional assignment of 'BookType'
conditions = [
    wc_book['TypeString'].str.contains('hardcover', case=False),
    wc_book['TypeString'].str.contains('audiobook', case=False),
    wc_book['TypeString'].str.contains('e-?book', case=False) | wc_book['Title'].str.contains('e-?book', case=False, na=False),
    wc_book['TypeString'].str.contains('paperback|print', case=False, na=False)
]
choices = ['Hardcover', 'Audiobook', 'E-Book', 'Print']

wc_book['BookType'] = np.select(conditions, choices, default='Print')

# 4. chained, vectorized .str.replace()
wc_book['Title'] = (
    wc_book['Title']
    .str.replace('&ndash; ', '', regex=False)
    .str.replace('(E-book)', '', regex=False)
    .str.replace(' <BR>&nbsp;<BR>', '', regex=False)
    .str.replace('#038; ', '', regex=False)
    .str.replace("'", "", regex=False)
)

# 5. Final cleaned dataframe
wc_clean = (
    wc_book[['Product Name', 'Title', 'BookType']]
    .rename(columns={'Product Name': 'SourceTitle', 'Title': 'CleanTitle'})
    .assign(Source='WooCommerce')
)

# Save to Stage
save_bucket(wc_clean, bucket_name, 'stage/woocom_stage/WooCom_Books_All.csv')

# Merch & Bundle to Stage
save_bucket(wc_merch, bucket_name, 'stage/woocom_stage/WooCom_Merch_All.csv')
save_bucket(wc_bundle, bucket_name, 'stage/woocom_stage/WooCom_Bundle_All.csv')

"""
SCB BOOK CLEAN AND SAVE
"""
#print(scb['Title'].unique)
scbb = scb_merge.groupby(['Title','ISBN']).size().reset_index().drop_duplicates()
#scb.head(10)
scbb = scbb[['Title']]
# For SCB, all titles that start with 'e' are E-Books
scbb.loc[(scbb['Title'].str.startswith(r'e', na = False)), 'BookType' ] = 'E-Book'
#booksinfodf['Number_of_Pages'] = booksinfodf['Number_of_Pages'].fillna(0)
scbb['BookType'] = scbb['BookType'].fillna("Print")
scbb['Title2'] = np.where(scbb['BookType'] == 'E-Book', scbb['Title'].str[1:], scbb['Title'])
scbb['Title2'] = scbb['Title2'].apply(lambda x : x.lstrip('e'))
scb_clean = scbb.rename(columns={'Title' : 'SourceTitle', 'Title2' : 'CleanTitle', 'BookType' : 'BookType'}).copy()
scb_clean['Source'] = 'SCB'

# 4. chained, vectorized .str.replace()
scb_clean['CleanTitle'] = (
    scb_clean['CleanTitle']
    .str.replace('COTTONMOUTN', 'COTTONMOUTH', regex=False)
    .str.replace('NEVER CARCH', 'NEVER CATCH', regex=False)
    .str.replace('SWEET YOUNG & WORRIES', 'SWEET YOUNG & WORRIED', regex=False)
    .str.replace("'", "", regex=False)
    .str.replace(",", "", regex=False)
    .str.replace("&", "", regex=False)
    .str.replace("  ", " ", regex=False)    
)

scb_clean = scb_clean[['SourceTitle', 'CleanTitle', 'BookType', 'Source']]

# Save to Stage
save_bucket(scb_clean, bucket_name, 'stage/scb_stage/SCB_Books_All.csv')
