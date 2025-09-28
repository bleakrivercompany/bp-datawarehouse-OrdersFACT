# -*- coding: utf-8 -*-
# book_dim

import pandas as pd
import numpy as np
import re
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from datetime import date
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize
# Custom Functions
from gcp_getsecrets import get_gcp_secret
from gcp_postbucket import save_bucket
from gcp_getbucket import get_bucket_csv
from gcp_getbigquery import read_bq_table
from gcp_savebigquery import save_to_bq

# Define project
project_id = f"button-datawarehouse"
# Define storage bucket for push
bucket_name = "cs-royalties-test"  # Replace with your bucket name
# Scopes for Service Account
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/admin.datatransfer",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]
# Define secrets to fetch
p_consumerkey = get_gcp_secret(project_id, "wc_consumer_key", "latest")
p_consumersecret = get_gcp_secret(project_id, "wc_consumer_secret", "latest")
secret_id_for_sa_key = "storage_sa_key" # The secret you just created
# get those secrets
sa_key_json_string = get_gcp_secret(project_id, secret_id_for_sa_key)
credentials_info = json.loads(sa_key_json_string)
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes = SCOPES)
storage_client = storage.Client(credentials=credentials, project=project_id)
bq_client = bigquery.Client(credentials=credentials)

""" 
0. BOOKS_INFO_SOURCE, BUNDLE_INFO_SOURCE
"""
# Big Query Book Master
books_info_database = "master_dim_data"
books_info_table = "books_info_source"
#booksinfo_df = read_bq_table(f"button-datawarehouse.master_dim_data.testtable", client=bq_client)
booksinfodf = read_bq_table(f"{project_id}.{books_info_database}.{books_info_table}", bq_client)

# Big Query Bundle Master
bundle_info_database = "master_dim_data"
bundle_info_table = "bundle_info_source"
#booksinfo_df = read_bq_table(f"button-datawarehouse.master_dim_data.testtable", client=bq_client)
bundleinfodf = read_bq_table(f"{project_id}.{bundle_info_database}.{bundle_info_table}", bq_client)

# Clean Books Info Source

booksinfodf['Audiobook_Royalty_Rate'] = booksinfodf['Audiobook_Royalty_Rate'].fillna('0.25')
booksinfodf['Print_Royalty_Rate'] = booksinfodf['Print_Royalty_Rate'].fillna('0.15')
booksinfodf['eBook_Royalty_Rate'] = booksinfodf['eBook_Royalty_Rate'].fillna('0.25')
booksinfodf['Print_ISBN'] = booksinfodf['Print_ISBN'].fillna('NA-Print-' + booksinfodf['Short_Hand_Title'])
booksinfodf['eBook_ISBN'] = booksinfodf['eBook_ISBN'].fillna('NA-eBook-' + booksinfodf['Short_Hand_Title'])
booksinfodf['Audiobook_ISBN'] = booksinfodf['Audiobook_ISBN'].fillna('NA-Audiobook-' + booksinfodf['Short_Hand_Title'])
booksinfodf['Hardcover_ISBN'] = booksinfodf['Hardcover_ISBN'].fillna('NA-Hardcover-' + booksinfodf['Short_Hand_Title'])
booksinfodf['Number_of_Pages'] = booksinfodf['Number_of_Pages'].fillna(0)

"""
1. START WITH SCB BOOKS AND WC BOOKS CLEAN
"""
# Set Dtype Dict

source_book_dtypes = { "SourceTitle" : object,
                      "CleanTitle" : object,
                      "BookType" :object,
                      "Source" :object
                      }

# Grab from Stage, set dtypes
scb_clean = get_bucket_csv(bucket_name, 'stage/scb_stage/SCB_Books_All.csv', dtype_spec=source_book_dtypes, client=storage_client).reset_index(drop=True).drop_duplicates()
wc_clean = get_bucket_csv(bucket_name, 'stage/woocom_stage/WooCom_Books_All.csv', dtype_spec=source_book_dtypes, client=storage_client).reset_index(drop=True).drop_duplicates()
# Merch & Bundle
wc_merch = get_bucket_csv(bucket_name, 'stage/woocom_stage/WooCom_Merch_All.csv', client=storage_client).reset_index(drop=True).drop_duplicates()
wc_bundle = get_bucket_csv(bucket_name, 'stage/woocom_stage/WooCom_Bundle_All.csv', client=storage_client).reset_index(drop=True).drop_duplicates()

"""
2. APPEND THE FRAMES TOGETHER
"""
wc_scb = pd.concat([wc_clean, scb_clean], ignore_index=True)
wc_scb['key'] = 0
booksinfodf['key'] = 0
cross_join = booksinfodf.merge(wc_scb, on='key', how='outer')
booksf = cross_join.copy()
booksf = booksf.set_index('Book_Title', drop=False)
booksf = booksf.rename(columns={'Book_Title': 'MasterTitle'})

# Initial cleaning step: strip leading and trailing spaces from Master and Clean
booksf['CleanTitle'] = booksf['CleanTitle'].apply(lambda x : x.strip())
booksf['MasterTitle'] = booksf['MasterTitle'].fillna('')
booksf['MasterTitle'] = booksf['MasterTitle'].apply(lambda x : x.strip())
booksf['TestTitle'] = booksf['CleanTitle']

# Word and string replacements
words_to_replace = r'\s+\b(the|and)\b\s+|\s*\|\s*'

# [():] matches any single character in the set
# The other words are matched directly
chars_and_phrases_to_remove = r"[():]|pre-order|pre order|paperback|hardcover|'"
# --- MasterTitleL chained cleaning operation ---
booksf['MasterTitleL'] = (
    booksf['MasterTitle']
    .str.lower()
    .str.replace(words_to_replace, ' ', regex=True)        # Replace ' the ', ' and ', '|' with a space
    .str.replace(chars_and_phrases_to_remove, '', regex=True) # Remove all other unwanted text
    .str.replace(r'\s+', ' ', regex=True)                  # Collapse multiple spaces into one
    .str.strip()                                           # Remove leading/trailing spaces
)

# --- TestTitleL chained cleaning operation ---
booksf['TestTitleL'] = (
    booksf['TestTitle']
    .str.lower()
    .str.replace(words_to_replace, ' ', regex=True)        # Replace ' the ', ' and ', '|' with a space
    .str.replace(chars_and_phrases_to_remove, '', regex=True) # Remove all other unwanted text
    .str.replace(r'\s+', ' ', regex=True)                  # Collapse multiple spaces into one
    .str.strip()                                           # Remove leading/trailing spaces
)

# Specific Cleanups
# Define a dictionary for these direct string replacements
replacements = {
    'future limited edition': 'future limited edition hilborn',
    'helium limited edition': 'helium limited edition francisco',
    'madness vase': 'madness vase gibson'
}

# Use pd.Series.replace() with the dictionary
# regex=True is crucial to replace these substrings anywhere in the title
booksf['TestTitleL'] = booksf['TestTitleL'].replace(replacements, regex=True)

# 2. Use .loc for the conditional update
# This is much faster than an if/else lambda
condition = (booksf['TestTitleL'] == 'poetry by chance')
booksf.loc[condition, 'TestTitleL'] += ' an anthology of poems powered by metaphor dice'

# Strip one more time to catch any whitepaces added during cleanup
booksf['TestTitleL'] = booksf['TestTitleL'].apply(lambda x : x.strip())
booksf['MasterTitleL'] = booksf['MasterTitleL'].apply(lambda x : x.strip())

""" 
3. FUZZY MATCH MASTER TO TEST
"""
booksf2 = booksf.copy()

# 1. Prepare the vectorizer's vocabulary
all_titles = pd.concat([booksf2['MasterTitleL'], booksf2['TestTitleL']]).dropna().unique()
vectorizer = TfidfVectorizer().fit(all_titles)

# 2. Create TF-IDF vectors, filling NaNs
vectors1 = vectorizer.transform(booksf2['MasterTitleL'].fillna(''))
vectors2 = vectorizer.transform(booksf2['TestTitleL'].fillna(''))

# 3. Calculate similarity
# Normalize the row vectors to unit length
vectors1_normalized = normalize(vectors1)
vectors2_normalized = normalize(vectors2)

# Compute the dot product of corresponding row vectors
# This is equivalent to cosine similarity for normalized vectors
similarity_scores = np.asarray(vectors1_normalized.multiply(vectors2_normalized).sum(axis=1)).flatten()

# 4. Add the new score to booksf2
booksf2['TfidfSimilarity'] = similarity_scores

# 5. Filter based on this highly contextual score
frat2 = booksf2['TfidfSimilarity'] >= 0.7 
bookfilt2 = booksf2[frat2].copy()

conditions = [
    (bookfilt2['BookType'] == 'Print') ,
    (bookfilt2['BookType'] == 'E-Book') ,
    (bookfilt2['BookType'] == 'Audiobook'),
    (bookfilt2['BookType'] == 'Hardcover') & ~(bookfilt2['Hardcover_ISBN'].str.contains(r'NA')),
    (bookfilt2['BookType'] == 'Hardcover') & (bookfilt2['Hardcover_ISBN'].str.contains(r'NA'))
    ]

choices = [bookfilt2['Print_ISBN'], bookfilt2['eBook_ISBN'], bookfilt2['Audiobook_ISBN'], bookfilt2['Hardcover_ISBN'], bookfilt2['Print_ISBN']]

bookfilt2['ISBN_All'] = np.select(conditions, choices, default='Missing')

""" 
4. FINAL DIM TABLE CONSTRUCTION
"""
bookdim_rename_map = {
    'ISBN_All' : 'ISBN_All', 
    'MasterTitle' : 'True_Title',
    'Author' : 'Author',
    'Royalty_Author_Name' : 'Royalty_Author_Name', 
    'Short_Hand_Title' : 'Short_Hand_Title', 
    'Pub_Date' : 'Pub_Date', 
    'Price' : 'Price', 
    'Number_of_Pages' : 'Number_of_Pages',
    'Book_Status' : 'Book_Status', 
    'SourceTitle' : 'Source_Title',
    'CleanTitle' : 'Clean_Title',
    'BookType' : 'Book_Type',
    'Print_Royalty_Rate' : 'Print_Royalty_Rate', 
    'eBook_Royalty_Rate' : 'eBook_Royalty_Rate',
    'Audiobook_Royalty_Rate' : 'Audiobook_Royalty_Rate', 
    'Source' : 'Source_System'
}

Book_Dim = (
    bookfilt2[list(bookdim_rename_map.keys())]
    .rename(columns=bookdim_rename_map)
)

conditions = [
    (Book_Dim['Book_Type'] == 'Print') ,
    (Book_Dim['Book_Type'] == 'E-Book') ,
    (Book_Dim['Book_Type'] == 'Audiobook'),
    (Book_Dim['Book_Type'] == 'Hardcover') & ~(Book_Dim['ISBN_All'].str.contains(r'NA')),
    (Book_Dim['Book_Type'] == 'Hardcover') & (Book_Dim['ISBN_All'].str.contains(r'NA'))
    ]

choices = [Book_Dim['Print_Royalty_Rate'], Book_Dim['eBook_Royalty_Rate'], Book_Dim['Audiobook_Royalty_Rate'], Book_Dim['Print_Royalty_Rate'], Book_Dim['Print_Royalty_Rate']]
Book_Dim['Royalty_Rate_All'] = np.select(conditions, choices, default=0)

Book_Dim['Royalty_Author_Name'] = Book_Dim['Royalty_Author_Name'].fillna('No Record')
Book_Dim['ISBN_All'] = Book_Dim['ISBN_All'].fillna('No Record')

#Book_Dim['Print_Royalty_Rate'] = Book_Dim['Print_Royalty_Rate'].apply(lambda x : x.replace('%', ''))
Book_Dim['Print_Royalty_Rate'] = [str(x).replace('%','') for x in Book_Dim['Print_Royalty_Rate'] ]
Book_Dim['eBook_Royalty_Rate'] = [str(x).replace('%','') for x in Book_Dim['eBook_Royalty_Rate'] ]
Book_Dim['Audiobook_Royalty_Rate'] = [str(x).replace('%','') for x in Book_Dim['Audiobook_Royalty_Rate'] ]
Book_Dim['Royalty_Rate_All'] = [str(x).replace('%','') for x in Book_Dim['Royalty_Rate_All'] ]

#DFC['ItemOrderSeq'] = DFC['ItemOrderSeq'].astype(int)
Book_Dim['Print_Royalty_Rate'] = Book_Dim['Print_Royalty_Rate'].astype(float)
Book_Dim['eBook_Royalty_Rate'] = Book_Dim['eBook_Royalty_Rate'].astype(float) 
Book_Dim['Audiobook_Royalty_Rate'] = Book_Dim['Audiobook_Royalty_Rate'].astype(float) 
Book_Dim['Royalty_Rate_All'] = Book_Dim['Royalty_Rate_All'].astype(float)

# List the columns you want to apply the logic to
cols_to_check = ['Print_Royalty_Rate', 'eBook_Royalty_Rate', 'Audiobook_Royalty_Rate', 'Royalty_Rate_All']

# Loop through the list of columns
for col in cols_to_check:
    Book_Dim.loc[Book_Dim[col] > 1.00, col] /= 100
""" 
FETCH MERCH AND BUNDLE
"""
Merch_Dim = wc_merch[['Product Name']].reset_index(drop=True)
# need the item ID, price, product name, order line subtotal
Merch_Dim = Merch_Dim.filter(['Product Name'], axis=1).drop_duplicates().reset_index(drop=True)
Merch_Dim = Merch_Dim.rename(columns={'Product Name' : 'Product_Name'})
#Bundle Dim
Bundle_Dim = wc_bundle[['Product Name', 'Bundle ID']].drop_duplicates().reset_index()
Bundle_Dim = Bundle_Dim.filter(['Product Name', 'Bundle ID'], axis=1)
Bundle_Dim = Bundle_Dim.rename(columns={'Product Name' : 'Product_Name', 'Bundle ID' : 'Bundle_ID'})
Bundle_Dim['Bundle_ID'] = Bundle_Dim['Bundle_ID'].fillna('Error - Missing'+ Bundle_Dim['Product_Name'])
# Match to Bundle Info Source
Bundle_Dim['key'] = 0
bundleinfodf['key'] = 0

crossb = bundleinfodf.merge(Bundle_Dim, on='key', how='outer')
bundlef = crossb.copy()
bundlef['Product'] = bundlef['Product'].fillna('')
bundlef.set_index('Product', inplace=True, drop=False)
bundlef = bundlef.rename(columns={'Product' : 'MasterBundle'})
bundlef['TestBundle'] = bundlef['Product_Name'].apply(lambda x : x.strip())
bundlef['TestBundle'] = bundlef['TestBundle'].str.lower()
bundlef['MasterBundle'] = bundlef['MasterBundle'].apply(lambda x : x.strip())
bundlef['MasterBundleL'] = bundlef['MasterBundle'].str.lower()

bundlef['FuzzyRatio'] = bundlef.apply(lambda s: fuzz.ratio(s['MasterBundleL'], s['TestBundle']), axis=1)
bundrat = bundlef['FuzzyRatio'] >= 95
bunfilt = bundlef[bundrat].copy()

bundle_rename_map = {
    'MasterBundle' : 'True_Bundle_Title',
    'Standard_Sale_Price' : 'Standard_Sale_Price',
    'Books' : 'Books',
    'Non_books' : 'Non-books',
    '__Revenue_Per_Book' : 'Revenue_Share_Book', 
    '__Revenue_to_Non_Books' : 'Revenue_Share_NonBook', 
    'Product_Type' : 'Product_Type',
    'Product_Name' : 'Source_Bundle_Title',
    'Bundle_ID' : 'Bundle_ID',
}

Bundle_Dim = (
    bunfilt[list(bundle_rename_map.keys())]
    .rename(columns=bundle_rename_map)
)

Bundle_Dim['Revenue_Share_Book'] = Bundle_Dim['Revenue_Share_Book'].astype(float)
Bundle_Dim['Revenue_Share_NonBook'] = Bundle_Dim['Revenue_Share_NonBook'].astype(float)
Bundle_Dim['Bundle_ID'] = [str(x).replace('.0','') for x in Bundle_Dim['Bundle_ID'] ]
Bundle_Dim['Bundle_ID']
#Bundle_Dim['Revenue_Share_Book'] = Bundle_Dim['Revenue_Share_Book'].astype(float)
#Bundle_Dim['Revenue_Share_NonBook'] = Bundle_Dim['Revenue_Share_NonBook'].astype(float)
# List the columns you want to apply the logic to
cols_to_check = ['Revenue_Share_NonBook', 'Revenue_Share_Book']

# Loop through the list of columns
for col in cols_to_check:
    Bundle_Dim.loc[Bundle_Dim[col] > 1.00, col] /= 100

# Final Dimension Tables are now stored in two locations: Cloud Storage Buckets and BigQuery
# Blobs
book_dim_blob = "dimension_tables/Book_Dim.csv"
save_bucket(Book_Dim, bucket_name, book_dim_blob, storage_client)
bundle_dim_blob = "dimension_tables/Bundle_Dim.csv"
save_bucket(Bundle_Dim, bucket_name, bundle_dim_blob, storage_client)
merch_dim_blob = "dimension_tables/Merch_Dim.csv"
save_bucket(Merch_Dim, bucket_name, merch_dim_blob, storage_client)

# BigQuery
# save_to_bq(df: pd.DataFrame, table_id: str, client=bigquery.Client(), write_disposition: str = 'WRITE_TRUNCATE'):
# "your-gcp-project-id.your_dataset_name.your_table_name"
# save_to_bq(scb_clean, f"{project_id}.{book_dim_database}.{book_dim_table}", bq_client)

book_dim_database = f"master_dim_data"
book_dim_table = f"Book_Dim"
save_to_bq(Book_Dim, f"{project_id}.{book_dim_database}.{book_dim_table}", bq_client)

bundle_dim_database = "master_dim_data"
bundle_dim_table = "Bundle_Dim"
save_to_bq(Bundle_Dim, f"{project_id}.{bundle_dim_database}.{bundle_dim_table}", bq_client)

merch_dim_database = "master_dim_data"
merch_dim_table = "Merch_Dim"
save_to_bq(Merch_Dim, f"{project_id}.{merch_dim_database}.{merch_dim_table}", bq_client)
