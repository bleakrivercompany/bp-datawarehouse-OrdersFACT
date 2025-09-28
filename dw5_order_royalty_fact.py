# -*- coding: utf-8 -*-
# FACT ROYALTY & FACT ORDER

import pandas as pd
import numpy as np
import json
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
# Custom Functions
from gcp_getsecrets import get_gcp_secret
from gcp_postbucket import save_bucket
from gcp_getbucket import get_bucket_csv
from gcp_savebigquery import save_to_bq
""" 
GCP PARAMETERS
"""
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
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
0. LOAD TABLES: SCB, WC, BOOKS, BUNDLES, MERCH, DATE
"""
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
# WooCom All
wcall_blob_name = "stage/woocom_stage/WooCom_Orders_Complete.csv"
wcall = get_bucket_csv(bucket_name, wcall_blob_name, dtype_spec=wc_dtypes, client = storage_client, date_cols=['Order Date']).reset_index(drop=True).drop_duplicates()
# Date formatting
wcall['Order Date'] = pd.to_datetime(wcall['Order Date']).dt.date
# SCB All
scb_blob_name = "stage/scb_stage/SCB_Complete.csv"
scb = get_bucket_csv(bucket_name, scb_blob_name, dtype_spec=scb_dtypes, client = storage_client).reset_index(drop=True)
# Blobs for DIM tables
book_dim_blob = "dimension_tables/Book_Dim.csv"
book = get_bucket_csv(bucket_name, book_dim_blob, client = storage_client)
bundle_dim_blob = "dimension_tables/Bundle_Dim.csv"
bundle = get_bucket_csv(bucket_name, bundle_dim_blob, client = storage_client)
merch_dim_blob = "dimension_tables/Merch_Dim.csv"
merch = get_bucket_csv(bucket_name, merch_dim_blob, client = storage_client)
date_dim_blob = "dimension_tables/Date_Dim.csv"
dd = get_bucket_csv(bucket_name, date_dim_blob, client = storage_client)

"""
SCB SALES
"""
scb['MKEY'] = scb['Month'] + scb['Year'].astype(str)

subdd = dd[['WC_Quarter', 'SCB_Sales_Qtr', 'SCB_Return_Qtr', 'monthyear']].reset_index(drop=True)

scb1 = pd.merge(scb, subdd, left_on=['MKEY'], right_on=['monthyear'], how='left').reset_index(drop=True)

subbook = book[
    ['Source_Title', 
     'ISBN_All' , 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Royalty_Rate_All'
     ]
    ]

scb2 = pd.merge(scb1, subbook, left_on=['Title'], right_on=['Source_Title'], how='left').reset_index(drop=True)

scb3 = scb2[
    ['Year', 
     'Month', 
     'MKEY', 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Title', 
     'Quantity Shipped', 
     'Publisher Payment', 
     'Quantity Returned', 
     'Publisher Credits', 
     'Royalty_Rate_All'
     ]].rename(columns=
               {'Quantity Shipped' : 'Quantity_Shipped', 
                'Publisher Payment' : 'Revenue_Gross'
                }).drop_duplicates().reset_index(drop=True)

scb3['True_Title'] = scb3['True_Title'].fillna('Missing - '+scb3['Title'])
scb3['ISBN_All'] = scb3['ISBN_All'].fillna('Missing -'+scb3['Title'])
scb3['Book_Type'] = scb3['Book_Type'].fillna('Error - Missing')
scb3['Author'] = scb3['Author'].fillna('NA')
scb3['Royalty_Author_Name'] = scb3['Royalty_Author_Name'].fillna('NA')

scb4 = scb3.groupby(
    ['Year',
     'Month',
     'MKEY',
     'WC_Quarter',
     'SCB_Sales_Qtr',
     'SCB_Return_Qtr',
     'ISBN_All',
     'Author',
     'Royalty_Author_Name',
     'True_Title',
     'Book_Type',
     'Royalty_Rate_All'
    ], as_index=False).agg(
        {'Quantity_Shipped': 'sum',
         'Revenue_Gross': 'sum'
        }).reset_index(drop=True)

scb4['Category'] = 'Book'
scb4['Data_Source'] = 'SCB Sales'
scb4['Discount_Amt'] = 0
scb4['Coupon_Code'] = 'NA'
scb4['Shipping_Revenue'] = 0
scb4['Gift_Wrap_Revenue'] = 0
scb4['Customer_Email_Address'] = 'scb@scbdistributors.com'
scb4['ShippingCity'] = 'Gardena'
scb4['ShippingState'] = 'CA'
scb4['ShippingZip'] = '90248'
scb4['Quantity_Returned'] = 0
scb4['Returns_in_Revenue'] = 0
scb4['Quantity_Wholesale'] = 0

"""
SCB RETURNS
"""
scbr1 = pd.merge(scb, subdd, left_on=['MKEY'], right_on=['monthyear'], how='left').reset_index(drop=True)

subbook = book[
    ['Source_Title', 
     'ISBN_All' , 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Royalty_Rate_All'
     ]]

scbr2 = pd.merge(scbr1, subbook, left_on=['Title'], right_on=['Source_Title'], how='left').reset_index(drop=True)
scbr3 = scbr2[
    ['Year', 
     'Month', 
     'MKEY', 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Title', 
     'Quantity Shipped', 
     'Publisher Payment', 
     'Quantity Returned', 
     'Publisher Credits', 
     'Royalty_Rate_All'
     ]].rename(columns=
               {'Publisher Credits' : 'Returns_in_Revenue', 
                'Quantity Returned' : 'Quantity_Returned'
                }).drop_duplicates().reset_index(drop=True)
scbr3['True_Title'] = scbr3['True_Title'].fillna('Missing - '+scbr3['Title'])
scbr3['ISBN_All'] = scbr3['ISBN_All'].fillna('Missing -'+scbr3['Title'])
scbr3['Book_Type'] = scbr3['Book_Type'].fillna('Error - Missing',)
scbr3['Author'] = scbr3['Author'].fillna('NA')
scbr3['Royalty_Author_Name'] = scbr3['Royalty_Author_Name'].fillna('NA')

scbr4 = scbr3.groupby(
    ['Year' , 
     'Month' , 
     'MKEY' , 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Royalty_Rate_All'
     ], as_index=False).agg(
         {'Quantity_Returned':'sum', 
          'Returns_in_Revenue': 'sum'
          }).reset_index(drop=True)

scbr4['Category'] = 'Book'
scbr4['Data_Source'] = 'SCB Returns'
scbr4['Discount_Amt'] = 0
scbr4['Coupon_Code'] = 'NA'
scbr4['Shipping_Revenue'] = 0
scbr4['Gift_Wrap_Revenue'] = 0
scbr4['Customer_Email_Address'] = 'scb@scbdistributors.com'
scbr4['ShippingCity'] = 'Gardena'
scbr4['ShippingState'] = 'CA'
scbr4['ShippingZip'] = '90248'
scbr4['Quantity_Shipped'] = 0 
scbr4['Revenue_Gross'] = 0
scbr4['Quantity_Wholesale'] = 0

"""
WOOCOM SALES
"""
wcall['Bundle ID'] = [str(x).replace('.0','') for x in wcall['Bundle ID'] ]
wcall['Bundled By'] = [str(x).replace('.0','') for x in wcall['Bundled By'] ]
# fill null
wcall['Bundled By'] = wcall['Bundled By'].fillna('Not Bundled')
# nan values
wcall['Bundle ID'] = wcall['Bundle ID'].replace({'nan':'Not Bundled'})
wcall['Bundled By'] = wcall['Bundled By'].replace({'nan': 'Not Bundled'})

wcall['JoinDate'] = pd.to_datetime(wcall['Order Date']).dt.date
dd['JoinDate'] = pd.to_datetime(dd['date']).dt.date

wc1 = pd.merge(wcall, dd, left_on=['JoinDate'], right_on=['JoinDate'], how='left').reset_index(drop=True)

# Select Columns and Reorder
wc2 = wc1[['Order Number', 
           'Order Status', 
           'Order Date', 
           'Total products', 
           'Total items', 
           'Order Subtotal Amount', 
           'Order Shipping Amount', 
           'Order Total Amount', 
           'Customer User Email', 
           'Customer Role', 
           'ShippingCity', 
           'ShippingState', 
           'ShippingZip', 
           'Country Name (Shipping)', 
           'Bundle ID', 
           'Bundled By', 
           'ItemOrderSeq', 
           'Product Name', 
           'Quantity', 
           'Order Line Subtotal', 
           'Item Discount Amount', 
           'Order Line Total', 
           'Category', 
           'Gift Wrap', 
           'Gift Wrap Cost', 
           'Coupon Code', 
           'Coupon Type', 
           'Coupon Amount', 
           'Discount Amount', 
           'monthname', 
           'year', 
           'monthyear', 
           'WC_Quarter', 
           'SCB_Sales_Qtr', 
           'SCB_Return_Qtr']].reset_index(drop=True)

wc3 = pd.merge(wc2, subbook, left_on=['Product Name'], right_on=['Source_Title'], how='left').drop_duplicates().reset_index(drop=True)

wc4 = pd.merge(wc3, bundle, left_on='Bundled By', right_on='Bundle_ID', how='left').drop_duplicates().reset_index(drop=True)

wc4['Bundled By'] = wc4['Bundled By'].fillna('Not Bundled')

conditions = [
    ~(wc4['Bundled By'] == 'Not Bundled') & (wc4['Category'] == 'Book')
    ]
choices = ['Bundled Book']
wc4['Category_Merge'] = np.select(conditions, choices, default = wc4['Category'])

# Merch
#merch = merch.drop_duplicates(subset='Product_Name')
wc5 = pd.merge(wc4, merch, left_on='Product Name', right_on='Product_Name', how='left').reset_index(drop=True)

wc5 = wc5.rename(columns=
           {'year' : 'Year', 
            'monthname' : 'Month', 
            'monthyear' : 'MKEY', 
            'Category' : 'OG_Category', 
            'Category_Merge' : 'Category'
            })
wc5.loc[(wc5['Gift Wrap'] == 'Yes'), 'Gift_Wrap_Line_Total'] = 3
wc5['Shipping_Per_Item'] = wc5['Order Shipping Amount'] / wc5['Total items']

conditions = [
   ( wc5['Category'] == 'Book') | (wc5['Category'] == 'Bundled Book')
    ]
choices = [wc5['True_Title']]
wc5['True_Title_2'] = np.select(conditions, choices, default=wc5['Product Name'])

# New Method for Assigning Revenue to Bundles
# The Bundle_Dim now contains a revenue share per book ratio that can be multiplied by the books

bundles_only = wc5['Category'] == 'Bundles'

bundle_of_join = wc5[bundles_only]

bundle_of_join = bundle_of_join[['Bundle ID', 
                                 'Order Line Total', 
                                 'Order Line Subtotal']]
# Create a small, unique DataFrame of bundle totals
unique_bundles = bundle_of_join.drop_duplicates(subset=['Bundle ID'])

wc5 = pd.merge(wc5, unique_bundles, left_on='Bundled By', right_on='Bundle ID', how='left')

# Order Line Total (net of discount, relevant to royalties)
conditions = [
    (wc5['Category'] == 'Bundles') & ~(wc5['Bundle ID_x'] == 'Yes') ,
    (wc5['Category'] == 'Merch') & ~(wc5['Bundle ID_x'] == 'Yes'),
    (wc5['Category'] == 'Bundled Book') & ~(wc5['Bundle ID_x'] == 'Yes')
    ]
choices = [ 0.0 , wc5['Revenue_Share_NonBook'] * wc5['Order Line Total_y'] , wc5['Revenue_Share_Book'] * wc5['Order Line Total_y']]

wc5['Bundle_Order_Line_Total'] = np.select(conditions, choices, default = 0.0)

# Order Line Subtotal (not net of discount, not for royalties)
conditions = [
    (wc5['Category'] == 'Bundles') & ~(wc5['Bundle ID_x'] == 'Yes') ,
    (wc5['Category'] == 'Merch') & ~(wc5['Bundle ID_x'] == 'Yes'),
    (wc5['Category'] == 'Bundled Book') & ~(wc5['Bundle ID_x'] == 'Yes')
    ]
choices = [ 0.0 , wc5['Revenue_Share_NonBook'] * wc5['Order Line Subtotal_y'] , wc5['Revenue_Share_Book'] * wc5['Order Line Subtotal_y']]

wc5['Bundle_Order_Line_Subtotal'] = np.select(conditions, choices, default = 0.0)
# Customer Role of Wholesale Customer is Excluded Revenue
conditions = [
    ( wc5['Customer Role'] == 'Wholesale Customer')
    ]
choices = [ wc5['Order Line Total_x'] ]
wc5['Revenue_Wholesale_Order_Line_Total'] = np.select(conditions, choices, default= 0.0)

conditions = [
    (wc5['Customer Role'] == 'Wholesale Customer')
    ]
choices = [ wc5['Order Line Subtotal_x'] ]
wc5['Revenue_Wholesale_Order_Line_Subtotal'] = np.select(conditions, choices, default= 0.0)

conditions = [
    (wc5['Customer Role'] == 'Wholesale Customer')
    ]
choices = [ wc5['Quantity'] ]
wc5['Quantity_Wholesale'] = np.select(conditions, choices, default= 0.0)

# Create a dictionary mapping fill values to the columns that need them
wc5_fill_values = {
    # Numeric columns to be filled with 0 or 0.0
    'Discount Amount': 0.0,
    'Gift_Wrap_Line_Total': 0.0,
    'Gift Wrap': 0.0,
    'Quantity': 0,
    'Quantity_Wholesale': 0,
    'Item Discount Amount': 0.0,
    'Shipping_Per_Item': 0.0,
    'Royalty_Rate_All': 0.0,
    'Bundle_Order_Line_Total': 0.0,
    'Bundle_Order_Line_Subtotal': 0.0,
    'Order Line Total_y': 0.0,
    'Order Line Subtotal_y': 0.0,
    'Order Line Total_x': 0.0,
    'Revenue_Wholesale_Order_Line_Total': 0.0,
    'Order Line Subtotal_x': 0.0,
    'Revenue_Wholesale_Order_Line_Subtotal': 0.0,

    # String columns for missing data or errors
    'True_Title_2': 'Error - Missing',
    'Category': 'Error - Missing',
    'Customer User Email': 'Error - Missing',

    # Columns to be filled with 'NA'
    'Author': 'NA',
    'Royalty_Author_Name': 'NA',
    'ShippingCity': 'NA',
    'ShippingState': 'NA',
    'ShippingZip': 'NA',

    # Quarter-related columns
    'WC_Quarter': '9999Q99',
    'SCB_Sales_Qtr': '9999Q99',
    'SCB_Return_Qtr': '9999Q99',
    
    # Coupon-related columns
    'Coupon Code': 'None Used',
    'Coupon Type': 'None',
    'Coupon Amount': 'None',
    'Customer Role': 'None',

    # Special date/key columns
    'Year': '9999',
    'Month': 'Smarch',
    'MKEY': 'Smarch9999'
}
# Apply all static fill
wc5 = wc5.fillna(value=wc5_fill_values)

# These must be done after 'Category' is filled.
wc5['Book_Type'] = wc5['Book_Type'].fillna(wc5['Category'])
wc5['ISBN_All'] = wc5['ISBN_All'].fillna(wc5['Category'])

""" 
WC5 is where FACT Order and FACT Royalty diverge

Order and Royalty serve different purposes and cannot be used to validate each other, most of the time

Order is used as a lookup table for all Orders down to the Line Item detail
Royalty is used to calculate different revenue summaries at the Book level

"""
# WC6 begins to group by fewer columns, leading to FACT Royalty's tighter grain
wc6 = wc5.groupby(
    ['Year', 
     'Month', 
     'MKEY', 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title_2', 
     'Book_Type', 
     'Category', 
     'Royalty_Rate_All'
     ], as_index=False).agg(
         {'Quantity':'sum', 
          'Quantity_Wholesale': 'sum', 
          'Order Line Subtotal_x' : 'sum', 
          'Item Discount Amount':'sum', 
          'Order Line Total_x': 'sum', 
          'Gift_Wrap_Line_Total' : 'sum', 
          'Shipping_Per_Item': 'sum', 
          'Bundle_Order_Line_Total': 'sum', 
          'Bundle_Order_Line_Subtotal': 'sum', 
          'Revenue_Wholesale_Order_Line_Total': 'sum', 
          'Revenue_Wholesale_Order_Line_Subtotal': 'sum'
          }).rename(columns=
                    {'Order Line Subtotal_x' : 'Revenue_Total_Product', 
                     'Order Line Total_x' : 'Revenue_Product_Net_Dct' , 
                     'Item Discount Amount' : 'Product_Discount_Amt', 
                     'Shipping_Per_Item' : 'Revenue_Shipping', 
                     'True_Title_2' : 'True_Title', 
                     'Gift_Wrap_Line_Total' : 'Revenue_Gift_Wrap'
                     }).reset_index(drop=True)
wc6 = wc6.sort_values(by=['Year', 'Month', 'True_Title'], ascending=[False, False, False]).reset_index(drop=True)
wc6['Data_Source'] = 'WC'

#SCB SALES
scb4['Bundle_Order_Line_Total'] = 0.0
scb4['Bundle_Order_Line_Subtotal'] = 0.0
scb4['Revenue_Wholesale_Order_Line_Total'] = 0.0
scb4['Revenue_Wholesale_Order_Line_Subtotal'] = 0.0
scb4['Revenue_Total_Product'] = scb4['Revenue_Gross']
scb4['Product_Discount_Amt'] = 0.0
scb4['Revenue_Product_Net_Dct'] = scb4['Revenue_Gross']
scb4['Revenue_Gift_Wrap'] = scb4['Gift_Wrap_Revenue']
scb4['Revenue_Shipping'] = scb4['Shipping_Revenue']
scb4['Quantity_Wholesale'] = 0

scb5 = scb4[['Year', #1
             'Month', #2 
             'MKEY', #3
             'WC_Quarter', #4
             'SCB_Sales_Qtr', #5
             'SCB_Return_Qtr', #6
             'ISBN_All', #7
             'Author', #8
             'Royalty_Author_Name', #8
             'True_Title', #9
             'Category', #10
             'Book_Type', #11
             'Royalty_Rate_All', #12
             'Quantity_Shipped', #13
             'Quantity_Returned', #14
             'Quantity_Wholesale', #15
             'Revenue_Total_Product', #16
             'Product_Discount_Amt', #17
             'Revenue_Product_Net_Dct', #18
             'Returns_in_Revenue', #19
             'Revenue_Gift_Wrap', #20
             'Revenue_Shipping', #21
             'Bundle_Order_Line_Total', #22
             'Bundle_Order_Line_Subtotal', #23
             'Revenue_Wholesale_Order_Line_Total', #24
             'Revenue_Wholesale_Order_Line_Subtotal', #25
             'Data_Source']] #26

# SCB Returns
scbr4['Bundle_Order_Line_Total'] = 0.0
scbr4['Bundle_Order_Line_Subtotal'] = 0.0
scbr4['Revenue_Wholesale_Order_Line_Total'] = 0.0
scbr4['Revenue_Wholesale_Order_Line_Subtotal'] = 0.0
scbr4['Revenue_Total_Product'] = scbr4['Revenue_Gross']
scbr4['Product_Discount_Amt'] = 0.0
scbr4['Revenue_Product_Net_Dct'] = scbr4['Revenue_Gross']
scbr4['Revenue_Gift_Wrap'] = scbr4['Gift_Wrap_Revenue']
scbr4['Revenue_Shipping'] = scbr4['Shipping_Revenue']
scbr4['Quantity_Wholesale'] = 0

scbr5 = scbr4[['Year', #1
             'Month', #2 
             'MKEY', #3
             'WC_Quarter', #4
             'SCB_Sales_Qtr', #5
             'SCB_Return_Qtr', #6
             'ISBN_All', #7
             'Author', #8
             'Royalty_Author_Name', #8
             'True_Title', #9
             'Category', #10
             'Book_Type', #11
             'Royalty_Rate_All', #12
             'Quantity_Shipped', #13
             'Quantity_Returned', #14
             'Quantity_Wholesale', #15
             'Revenue_Total_Product', #16
             'Product_Discount_Amt', #17
             'Revenue_Product_Net_Dct', #18
             'Returns_in_Revenue', #19
             'Revenue_Gift_Wrap', #20
             'Revenue_Shipping', #21
             'Bundle_Order_Line_Total', #22
             'Bundle_Order_Line_Subtotal', #23
             'Revenue_Wholesale_Order_Line_Total', #24
             'Revenue_Wholesale_Order_Line_Subtotal', #25
             'Data_Source']] #26

#Combine SCB
scb_fin = pd.concat([scb5, scbr5]).reset_index(drop=True)

wc6['Quantity_Shipped'] = wc6['Quantity']
wc6['Quantity_Returned'] = 0
wc6['Returns_in_Revenue'] = 0

wc7 = wc6[['Year', #1
           'Month', #2
           'MKEY', #3
           'WC_Quarter', #4
           'SCB_Sales_Qtr', #5
           'SCB_Return_Qtr', #6
           'ISBN_All', #7
           'Author' , #8
           'Royalty_Author_Name', #8
           'True_Title', #9
           'Category', #10
           'Book_Type', #11
           'Royalty_Rate_All', #12
           'Quantity_Shipped', #13
           'Quantity_Returned', #14
           'Quantity_Wholesale', #15
           'Revenue_Total_Product', #16
           'Product_Discount_Amt', #17
           'Revenue_Product_Net_Dct', #18
           'Returns_in_Revenue', #19
           'Revenue_Gift_Wrap', #20
           'Revenue_Shipping', #21
           'Bundle_Order_Line_Total', #22
           'Bundle_Order_Line_Subtotal', #23
           'Revenue_Wholesale_Order_Line_Total', #24
           'Revenue_Wholesale_Order_Line_Subtotal', #25
           'Data_Source']] #26

fr0 = pd.concat([scb_fin, wc7]).reset_index(drop=True)

fr0 = fr0.sort_values(by=['Year', 'Month', 'True_Title'], ascending=[False, False, False]).reset_index(drop=True)

conditions = [
   (fr0['Data_Source'] == 'WC') & ( (fr0['Category'] == 'Book' ) | (fr0['Category'] == 'Bundled Book' ))
    ]
choices = [ fr0['Quantity_Shipped'] - fr0['Quantity_Wholesale'] ]
fr0['Quantity_WC_Retail'] = np.select(conditions, choices, default=0)

conditions = [
    (fr0['Data_Source'] == 'SCB Sales')
    ]
choices = [fr0['Quantity_Shipped']]
fr0['Quantity_SCB_Sales'] = np.select(conditions, choices, default=0)

conditions = [
    (fr0['Data_Source'] == 'SCB Returns')
    ]
choices = [fr0['Quantity_Returned']]
fr0['Quantity_SCB_Returns'] = np.select(conditions, choices, default=0)

fr0['Quantity_Total'] = fr0['Quantity_Shipped'] - fr0['Quantity_Returned']
fr0['Revenue_Wholesale'] = fr0['Revenue_Wholesale_Order_Line_Total']
# Net Product Revenue minus Wholesale = WC Individual Retail : books only
conditions = [
   (fr0['Data_Source'] == 'WC') & (fr0['Category'] == 'Book' ) 
    ]
choices = [ fr0['Revenue_Product_Net_Dct'] - fr0['Revenue_Wholesale_Order_Line_Total']]
fr0['Revenue_WC_Ind_Retail'] = np.select(conditions, choices, default=0)

conditions = [
    (fr0['Data_Source'] == 'SCB Sales')
    ]
choices = [fr0['Revenue_Product_Net_Dct']]
fr0['Revenue_SCB_Ind_Retail'] = np.select(conditions, choices, default=0)
fr0['Revenue_Total_Ind_Retail'] = fr0['Revenue_WC_Ind_Retail'] + fr0['Revenue_SCB_Ind_Retail']

conditions = [
    fr0['Royalty_Rate_All'] > 0.0
    ]
choices = [ fr0['Bundle_Order_Line_Total'] ]
fr0['Revenue_WC_Bundle_Retail_RQ'] = np.select(conditions, choices, default=0)

fr0['Revenue_WC_Bundle_Retail'] = fr0['Bundle_Order_Line_Total']
fr0['Revenue_SCB_Returns'] = fr0['Returns_in_Revenue']
fr0['Revenue_WC_Royalty_Qualified'] = fr0['Revenue_WC_Ind_Retail'] + fr0['Revenue_WC_Bundle_Retail_RQ']
# this includes merch and bundles. Fix
fr0['Revenue_Total_All'] = fr0['Revenue_Product_Net_Dct'] + fr0['Revenue_Gift_Wrap'] + fr0['Revenue_Shipping'] - fr0['Returns_in_Revenue']

# WooCom Royalty Qualified Quantity
# Only books will have a non-zero royalty rate
conditions = [
    fr0['Royalty_Rate_All'] > 0.0
    ]
choices = [ fr0['Quantity_Shipped'] - fr0['Quantity_Wholesale'] - fr0['Quantity_Returned'] ]
fr0['Royalty_Qualified_Quantity'] = np.select(conditions, choices, default=0)

# Only Books will have a non-zero royalty rate
conditions = [
    fr0['Royalty_Rate_All'] > 0.0
    ]
choices = [ fr0['Revenue_Total_Ind_Retail'] + fr0['Revenue_WC_Bundle_Retail'] - fr0['Returns_in_Revenue'] ]
fr0['Royalty_Qualified_Revenue'] = np.select(conditions, choices, default=0)

fr0['Royalty_Paid'] = fr0['Royalty_Qualified_Revenue'] * fr0['Royalty_Rate_All']

fr1 = fr0

# Check for NULL values
fr1.isnull().sum(axis = 0)

conditions = [
   (fr1['Data_Source'] == 'WC'),
   (fr1['Data_Source'] == 'SCB Sales'),
   (fr1['Data_Source'] == 'SCB Returns')
    ]
choices = [fr1['WC_Quarter'], fr1['SCB_Sales_Qtr'], fr1['SCB_Return_Qtr'] ]
fr1['Combined_Quarter'] = np.select(conditions, choices, default = fr1['WC_Quarter'])

fr1['MKEY_Date'] = (pd.to_datetime(fr1['Year'].astype(str) + fr1['Month'], format='%Y%B'))

#combined mkey date
conditions = [
   (fr1['Data_Source'] == 'WC'),
   (fr1['Data_Source'] == 'SCB Sales'),
   (fr1['Data_Source'] == 'SCB Returns')
    ]
choices = [ fr1['MKEY_Date'] , fr1['MKEY_Date'] + pd.DateOffset(months=3), fr1['MKEY_Date'] ]

fr1['Combined_Month_SCB_fwd'] = np.select(conditions, choices, default=fr1['MKEY_Date'] )
fr1['Retail_Other_Sales'] = 0
fr1['Qty_Other_Sales'] = 0

"""
FR2 is the summary sales report
"""
fr2 = fr1.groupby(['Combined_Quarter', 'Author', 'Royalty_Author_Name'], as_index=False).agg({'Royalty_Qualified_Quantity' : 'sum', 
                                                                      'Royalty_Qualified_Revenue' : 'sum', 
                                                                      'Revenue_WC_Royalty_Qualified' : 'sum', 
                                                                      'Revenue_SCB_Ind_Retail' : 'sum', 
                                                                      'Revenue_SCB_Returns' : 'sum', 
                                                                      'Retail_Other_Sales' : 'sum', 
                                                                      'Quantity_SCB_Sales': 'sum',
                                                                      'Quantity_SCB_Returns': 'sum',
                                                                      'Quantity_Wholesale': 'sum',
                                                                      'Quantity_WC_Retail': 'sum',
                                                                      'Royalty_Paid' : 'sum'}).reset_index(drop=True).sort_values('Royalty_Author_Name')

fr2 = fr2.rename(columns={'Royalty_Qualified_Quantity' :'Total_Qty',
                  'Royalty_Qualified_Revenue' : 'Total_Item_USD',
                  'Revenue_WC_Royalty_Qualified' : 'BPcom_Sales_USD',
                  'Revenue_SCB_Ind_Retail' : 'SCB_Sales_USD',
                  'Revenue_SCB_Returns' : 'SCB_Returns',
                  'Retail_Other_Sales' : 'Other_USD',
                  'Quantity_SCB_Sales' : 'SCB_Sales_Quantity',
                  'Quantity_SCB_Returns' : 'SCB_Returns_Quantity',
                  'Quantity_Wholesale' : 'SCB_Wholesale_Quantity',
                  'Quantity_WC_Retail' : 'WooCom_Sales_Quantity',
                  'Royalty_Paid' : 'Payout'})

"""
FR4 is the Printable Sales Report. This can also work as the Summary, obviously
"""
fr4 = fr1.groupby(['Combined_Quarter', 
                   'Author', 
                   'Royalty_Author_Name', 
                   'Combined_Month_SCB_fwd', 
                   'True_Title', 
                   'Book_Type',
                   'Category',
                   'Royalty_Rate_All'], as_index=False).agg({'Royalty_Qualified_Quantity' : 'sum', 
                                                             'Royalty_Qualified_Revenue' : 'sum', 
                                                             'Revenue_WC_Royalty_Qualified' : 'sum', 
                                                             'Revenue_SCB_Ind_Retail' : 'sum', 
                                                             'Revenue_SCB_Returns' : 'sum', 
                                                             'Retail_Other_Sales' : 'sum',
                                                             'Quantity_SCB_Sales': 'sum',
                                                             'Quantity_SCB_Returns': 'sum',
                                                             'Quantity_Wholesale': 'sum',
                                                             'Quantity_WC_Retail': 'sum',
                                                             'Royalty_Paid' : 'sum'}).reset_index(drop=True).sort_values('Royalty_Author_Name')
                                                            
fr4 = fr4.rename(columns={'Royalty_Qualified_Quantity' :'Total_Qty',
                  'Royalty_Qualified_Revenue' : 'Total_Item_USD',
                  'Revenue_WC_Royalty_Qualified' : 'BPcom_Sales_USD',
                  'Revenue_SCB_Ind_Retail' : 'SCB_Sales_USD',
                  'Revenue_SCB_Returns' : 'SCB_Returns',
                  'Retail_Other_Sales' : 'Other_USD',
                  'Quantity_SCB_Sales' : 'SCB_Sales_Quantity',
                  'Quantity_SCB_Returns' : 'SCB_Returns_Quantity',
                  'Quantity_Wholesale' : 'SCB_Wholesale_Quantity',
                  'Quantity_WC_Retail' : 'WooCom_Sales_Quantity',
                  'Royalty_Paid' : 'Payout'})

"""
SPLIT ROYALTIES

Applies to Hawley & Erkkinen and Tyabji, Azura & Neal, Jackson

The "Author" column of Book Dim will be deprecated in the near future, so we need
to fix Hawley & Erkkinen. Then, we need to go back and fix all references to Author as a column in
Book Dim and the rest
"""
splitroy = (fr4['Royalty_Author_Name'] == "Erkinnen, Joel & Hawley, Shane") | (fr4['Royalty_Author_Name'] == "Tyabji, Azura & Neal, Jackson")
regroy = (fr4['Royalty_Author_Name'] != "Erkinnen, Joel & Hawley, Shane") & (fr4['Royalty_Author_Name'] != "Tyabji, Azura & Neal, Jackson")
# No Haw & Erk
fr5 = fr4[regroy].reset_index(drop=True)
fr5.groupby(['Royalty_Author_Name']).size()
# Only Haw & Erk
fr6 = fr4[splitroy].reset_index(drop=True)
fr6.groupby(['Royalty_Author_Name']).size()
# Dupe the frames
fr7 = fr6.copy()
fr8 = fr6.copy()
fr9 = fr6.copy()
fr10 = fr6.copy()
# Rename author in fr6 to Hawley, Shane
fr7['Split_Author'] = fr7['Royalty_Author_Name'].map(lambda x: x.replace("Erkinnen, Joel & Hawley, Shane", "Hawley, Shane"))
# Rename author in fr7 to Erkkinen, Joel
fr8['Split_Author'] = fr8['Royalty_Author_Name'].map(lambda y: y.replace("Erkinnen, Joel & Hawley, Shane", "Erkkinen, Joel"))
# Rename author in fr9 to Tyabji, Azura
fr9['Split_Author'] = fr9['Royalty_Author_Name'].map(lambda a: a.replace("Tyabji, Azura & Neal, Jackson", "Tyabji, Azura"))
# Rename author in fr10 to Neal, Jackson
fr10['Split_Author'] = fr10['Royalty_Author_Name'].map(lambda b: b.replace("Tyabji, Azura & Neal, Jackson", "Neal, Jackson"))
frs = pd.concat((fr7, fr8, fr9, fr10)).reset_index(drop=True)

# Save to Buckets and BigQuery
# CS
# FR1 : FACT Royalty
fact_royalty_blob = "fact_tables/FACT_Royalty.csv"
save_bucket(fr1, bucket_name, fact_royalty_blob, storage_client)
# FR2 : FACT Royalty Summary
fact_royalty_blob = "reporting_tables/Royalty_Summary_Report_Complete.csv"
save_bucket(fr2, bucket_name, fact_royalty_blob, storage_client)
# FR5: ROYALTY SALES
fact_royalty_blob = "reporting_tables/Royalties_Sales_Report_Complete.csv"
save_bucket(fr5, bucket_name, fact_royalty_blob, storage_client)
# FR5 : ROYALTY PRINTABLE
fact_royalty_blob = "reporting_tables/Printable_Royalties_Report_Complete.csv"
save_bucket(fr5, bucket_name, fact_royalty_blob, storage_client)
# FR6 : SPLIT SALES
fact_royalty_blob = "reporting_tables/Split_Royalties_Sales_Report_Complete.csv"
save_bucket(fr6, bucket_name, fact_royalty_blob, storage_client)
# FRS : SPLIT PRINTABLE
fact_royalty_blob = "reporting_tables/Split_Printable_Royalties_Report_Complete.csv"
save_bucket(frs, bucket_name, fact_royalty_blob, storage_client)
# BQ
# FR1 : FACT Royalty
fact_database = f"fact_tables"
royalty_fact_table = f"FACT_Royalty"
save_to_bq(fr1, f"{project_id}.{fact_database}.{royalty_fact_table}", bq_client)
# FR2 : FACT Royalty Summary
rpt_database = f"reporting_tables"
royalty_fact_table = f"Royalty_Summary_Report_Complete"
save_to_bq(fr2, f"{project_id}.{rpt_database}.{royalty_fact_table}", bq_client)
# FR5 : Royalty Sales
rpt_database = f"reporting_tables"
royalty_fact_table = f"Royalties_Sales_Report_Complete"
save_to_bq(fr5, f"{project_id}.{rpt_database}.{royalty_fact_table}", bq_client)
# FR5 : Printable Royalty Sales
rpt_database = f"reporting_tables"
royalty_fact_table = f"Printable_Royalties_Report_Complete"
save_to_bq(fr5, f"{project_id}.{rpt_database}.{royalty_fact_table}", bq_client)
# FR6 : SPLIT SALES
rpt_database = f"reporting_tables"
royalty_fact_table = f"Split_Royalties_Sales_Report_Complete"
save_to_bq(fr6, f"{project_id}.{rpt_database}.{royalty_fact_table}", bq_client)
# FRS : Printable SPLIT SALES
rpt_database = f"reporting_tables"
royalty_fact_table = f"Split_Printable_Royalties_Report_Complete"
save_to_bq(frs, f"{project_id}.{rpt_database}.{royalty_fact_table}", bq_client)

""" 
FACT ORDER

"""
wcfo1 = wc5.groupby([
    'Year', 
    'Month', 
    'MKEY', 
    'WC_Quarter', 
    'SCB_Sales_Qtr', 
    'SCB_Return_Qtr' ,
    'Order Number',
    'ItemOrderSeq', 
    'ISBN_All', 
    'Author', 
    'Royalty_Author_Name', 
    'True_Title_2', 
    'Book_Type', 
    'Category', 
    'Royalty_Rate_All',
    'Customer User Email', 
    'Customer Role', 
    'ShippingCity', 
    'ShippingState', 
    'ShippingZip', 
    'Country Name (Shipping)', 
    'Coupon Code', 
    'Coupon Type', 
    'Coupon Amount'
    ], as_index=False).agg(
                       {'Quantity':'sum', 
                        'Quantity_Wholesale': 'sum', 
                        'Order Line Subtotal_x' : 'sum', 
                        'Item Discount Amount':'sum',
                        'Order Line Total_x': 'sum',
                        'Gift_Wrap_Line_Total' : 'sum', 
                        'Shipping_Per_Item': 'sum', 
                        'Bundle_Order_Line_Total': 'sum', 
                        'Bundle_Order_Line_Subtotal': 'sum', 
                        'Revenue_Wholesale_Order_Line_Total': 'sum', 
                        'Revenue_Wholesale_Order_Line_Subtotal': 'sum'}
                       ).rename(columns=
                                {
                                'Order Line Subtotal_x' : 'Revenue_Total_Product', 
                                'Order Line Total_x' : 'Revenue_Product_Net_Dct' ,  
                                'Item Discount Amount' : 'Product_Discount_Amt', 
                                'Shipping_Per_Item' : 'Revenue_Shipping', 
                                'True_Title_2' : 'True_Title', 
                                'Gift_Wrap_Line_Total' : 'Revenue_Gift_Wrap'
                                }
                                ).reset_index(drop=True)
wcfo1 = wcfo1.sort_values(by=['Year', 'Month', 'True_Title'], ascending=[False, False, False]).reset_index(drop=True)

wcfo1['Data_Source'] = 'WC'

#FACT ORDER SCB
scb4_fo = scb3.groupby(
    ['Year' , 
     'Month' , 
     'MKEY' , 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Royalty_Rate_All'
     ], as_index=False).agg({'Quantity_Shipped': "sum", 'Revenue_Gross': "sum"}).reset_index(drop=True)
scb4_fo['Category'] = 'Book'
scb4_fo['Data_Source'] = 'SCB Sales'
scb4_fo['Discount_Amt'] = 0
scb4_fo['Coupon Code'] = 'NA'
scb4_fo['Shipping_Revenue'] = 0
scb4_fo['Gift_Wrap_Revenue'] = 0
scb4_fo['Customer User Email'] = 'scb@scbdistributors.com'
scb4_fo['ShippingCity'] = 'Gardena'
scb4_fo['ShippingState'] = 'CA'
scb4_fo['ShippingZip'] = '90248'
scb4_fo['Quantity_Returned'] = 0
scb4_fo['Returns_in_Revenue'] = 0
# New
scb4_fo['Coupon Type'] = 'NA'
scb4_fo['Coupon Amount'] = 0
scb4_fo['ItemOrderSeq'] = 1
scb4_fo['Order Number'] = 'SCBR9999'
scb4_fo['Customer Role'] = 'Distributor'
scb4_fo['Country Name (Shipping)'] = 'USA'

#SCB SALES
scbfo1 = scb4_fo.copy()
scbfo1['Bundle_Order_Line_Total'] = 0.0
scbfo1['Bundle_Order_Line_Subtotal'] = 0.0
scbfo1['Revenue_Wholesale_Order_Line_Total'] = 0.0
scbfo1['Revenue_Wholesale_Order_Line_Subtotal'] = 0.0
scbfo1['Revenue_Total_Product'] = scbfo1['Revenue_Gross']
scbfo1['Product_Discount_Amt'] = 0.0
scbfo1['Revenue_Product_Net_Dct'] = scbfo1['Revenue_Gross']
scbfo1['Revenue_Gift_Wrap'] = scbfo1['Gift_Wrap_Revenue']
scbfo1['Revenue_Shipping'] = scbfo1['Shipping_Revenue']
scbfo1['Quantity_Wholesale'] = 0
scbfo1['Quantity_Returned'] = 0
scbfo1['Returns_in_Revenue'] = 0

scbfo2 = scbfo1[['Year', #1
             'Month', #2 
             'MKEY', #3
             'WC_Quarter', #4
             'SCB_Sales_Qtr', #5
             'SCB_Return_Qtr', #6
             'Order Number', #A
             'ItemOrderSeq', #B
             'Customer User Email', #C
             'Customer Role', #D
             'ShippingCity', #E
             'ShippingState', #F
             'ShippingZip', #G,
             'Country Name (Shipping)', #H
             'Coupon Code', #I
             'Coupon Type', #J
             'Coupon Amount', #K
             'ISBN_All', #7
             'Author', #8
             'Royalty_Author_Name', #8
             'True_Title', #9
             'Category', #10
             'Book_Type', #11
             'Royalty_Rate_All', #12
             'Quantity_Shipped', #13
             'Quantity_Returned', #14
             'Quantity_Wholesale', #15
             'Revenue_Total_Product', #16
             'Product_Discount_Amt', #17
             'Revenue_Product_Net_Dct', #18
             'Returns_in_Revenue', #19
             'Revenue_Gift_Wrap', #20
             'Revenue_Shipping', #21
             'Bundle_Order_Line_Total', #22
             'Bundle_Order_Line_Subtotal', #23
             'Revenue_Wholesale_Order_Line_Total', #24
             'Revenue_Wholesale_Order_Line_Subtotal', #25
             'Data_Source']] #26

# SCB Returns
scbr4_fo = scbr3.groupby(
    ['Year', 
     'Month', 
     'MKEY', 
     'WC_Quarter', 
     'SCB_Sales_Qtr', 
     'SCB_Return_Qtr', 
     'ISBN_All', 
     'Author', 
     'Royalty_Author_Name', 
     'True_Title', 
     'Book_Type', 
     'Royalty_Rate_All'
     ], as_index=False).agg({'Quantity_Returned':"sum", 'Returns_in_Revenue':"sum"}).reset_index(drop=True)
scbr4_fo['Category'] = 'Book'
scbr4_fo['Data_Source'] = 'SCB Returns'
scbr4_fo['Discount_Amt'] = 0
scbr4_fo['Coupon Code'] = 'NA'
scbr4_fo['Shipping_Revenue'] = 0
scbr4_fo['Gift_Wrap_Revenue'] = 0
scbr4_fo['Customer User Email'] = 'scb@scbdistributors.com'
scbr4_fo['ShippingCity'] = 'Gardena'
scbr4_fo['ShippingState'] = 'CA'
scbr4_fo['ShippingZip'] = '90248'
scbr4_fo['Quantity_Shipped'] = 0 
scbr4_fo['Revenue_Gross'] = 0
scbr4_fo['Quantity_Wholesale'] = 0
# New
scbr4_fo['Coupon Amount'] = 0
scbr4_fo['Coupon Type'] = 'NA'
scbr4_fo['ItemOrderSeq'] = 1
scbr4_fo['Order Number'] = 'SCBR9999'
scbr4_fo['Customer Role'] = 'Distributor'
scbr4_fo['Country Name (Shipping)'] = 'USA'
# CHECK
scbrfo1 = scbr4_fo.copy()
scbrfo1['Bundle_Order_Line_Total'] = 0.0
scbrfo1['Bundle_Order_Line_Subtotal'] = 0.0
scbrfo1['Revenue_Wholesale_Order_Line_Total'] = 0.0
scbrfo1['Revenue_Wholesale_Order_Line_Subtotal'] = 0.0
scbrfo1['Revenue_Total_Product'] = scbrfo1['Revenue_Gross']
scbrfo1['Product_Discount_Amt'] = 0.0
scbrfo1['Revenue_Product_Net_Dct'] = scbrfo1['Revenue_Gross']
scbrfo1['Revenue_Gift_Wrap'] = scbrfo1['Gift_Wrap_Revenue']
scbrfo1['Revenue_Shipping'] = scbrfo1['Shipping_Revenue']
scbrfo1['Quantity_Wholesale'] = 0
scbrfo1['Quantity_Shipped'] = 0 

scbrfo2 = scbrfo1[['Year', #1
             'Month', #2 
             'MKEY', #3
             'WC_Quarter', #4
             'SCB_Sales_Qtr', #5
             'SCB_Return_Qtr', #6
             'Order Number', #A
             'ItemOrderSeq', #B
             'Customer User Email', #C
             'Customer Role', #D
             'ShippingCity', #E
             'ShippingState', #F
             'ShippingZip', #G,
             'Country Name (Shipping)', #H
             'Coupon Code', #I
             'Coupon Type', #J
             'Coupon Amount', #K
             'ISBN_All', #7
             'Author', #8
             'Royalty_Author_Name', #8
             'True_Title', #9
             'Category', #10
             'Book_Type', #11
             'Royalty_Rate_All', #12
             'Quantity_Shipped', #13
             'Quantity_Returned', #14
             'Quantity_Wholesale', #15
             'Revenue_Total_Product', #16
             'Product_Discount_Amt', #17
             'Revenue_Product_Net_Dct', #18
             'Returns_in_Revenue', #19
             'Revenue_Gift_Wrap', #20
             'Revenue_Shipping', #21
             'Bundle_Order_Line_Total', #22
             'Bundle_Order_Line_Subtotal', #23
             'Revenue_Wholesale_Order_Line_Total', #24
             'Revenue_Wholesale_Order_Line_Subtotal', #25
             'Data_Source']] #26

scb_fofin = pd.concat([scbfo2, scbrfo2]).reset_index(drop=True)

wcfo1['Quantity_Shipped'] = wcfo1['Quantity']
wcfo1['Quantity_Returned'] = 0
wcfo1['Returns_in_Revenue'] = 0

wcfo2 = wcfo1[['Year', #1
           'Month', #2
           'MKEY', #3
           'WC_Quarter', #4
           'SCB_Sales_Qtr', #5
           'SCB_Return_Qtr', #6
           'Order Number', #A
           'ItemOrderSeq', #B
           'Customer User Email', #C
           'Customer Role', #D
           'ShippingCity', #E
           'ShippingState', #F
           'ShippingZip', #G,
           'Country Name (Shipping)', #H
           'Coupon Code', #I
           'Coupon Type', #J
           'Coupon Amount', #K
           'ISBN_All', #7
           'Author' , #8
           'Royalty_Author_Name', #8
           'True_Title', #9
           'Category', #10
           'Book_Type', #11
           'Royalty_Rate_All', #12
           'Quantity_Shipped', #13
           'Quantity_Returned', #14
           'Quantity_Wholesale', #15
           'Revenue_Total_Product', #16
           'Product_Discount_Amt', #17
           'Revenue_Product_Net_Dct', #18
           'Returns_in_Revenue', #19
           'Revenue_Gift_Wrap', #20
           'Revenue_Shipping', #21
           'Bundle_Order_Line_Total', #22
           'Bundle_Order_Line_Subtotal', #23
           'Revenue_Wholesale_Order_Line_Total', #24
           'Revenue_Wholesale_Order_Line_Subtotal', #25
           'Data_Source']] #26

fo1 = pd.concat([scb_fofin, wcfo2]).reset_index(drop=True)

fo1 = fo1.sort_values(by=['Year', 'Month', 'True_Title'], ascending=[False, False, False]).reset_index(drop=True)

conditions = [
   (fo1['Data_Source'] == 'WC') & ( (fo1['Category'] == 'Book' ) | (fo1['Category'] == 'Bundled Book' ))
    ]
choices = [ fo1['Quantity_Shipped'] - fo1['Quantity_Wholesale'] ]
fo1['Quantity_WC_Retail'] = np.select(conditions, choices, default=0)

conditions = [
    (fo1['Data_Source'] == 'SCB Sales')
    ]
choices = [fo1['Quantity_Shipped']]
fo1['Quantity_SCB_Sales'] = np.select(conditions, choices, default=0)

conditions = [
    (fo1['Data_Source'] == 'SCB Returns')
    ]
choices = [fo1['Quantity_Returned']]
fo1['Quantity_SCB_Returns'] = np.select(conditions, choices, default=0)
fo1['Quantity_Total'] = fo1['Quantity_Shipped'] - fo1['Quantity_Returned']
fo1['Revenue_Wholesale'] = fo1['Revenue_Wholesale_Order_Line_Total']
# Net Product Revenue minus Wholesale = WC Individual Retail : books only
conditions = [
   (fo1['Data_Source'] == 'WC') & (fo1['Category'] == 'Book' ) 
    ]
choices = [ fo1['Revenue_Product_Net_Dct'] - fo1['Revenue_Wholesale_Order_Line_Total']]
fo1['Revenue_WC_Ind_Retail'] = np.select(conditions, choices, default=0)
conditions = [
    (fo1['Data_Source'] == 'SCB Sales')
    ]
choices = [fo1['Revenue_Product_Net_Dct']]
fo1['Revenue_SCB_Ind_Retail'] = np.select(conditions, choices, default=0)
fo1['Revenue_Total_Ind_Retail'] = fo1['Revenue_WC_Ind_Retail'] + fo1['Revenue_SCB_Ind_Retail']
# need to exclude merch? I don't recall if this excludes Merch as a matter of course
conditions = [
    fo1['Royalty_Rate_All'] > 0.0
    ]
choices = [ fo1['Bundle_Order_Line_Total'] ]
fo1['Revenue_WC_Bundle_Retail_RQ'] = np.select(conditions, choices, default=0)
fo1['Revenue_WC_Bundle_Retail'] = fo1['Bundle_Order_Line_Total']
fo1['Revenue_SCB_Returns'] = fo1['Returns_in_Revenue']
fo1['Revenue_WC_Royalty_Qualified'] = fo1['Revenue_WC_Ind_Retail'] + fo1['Revenue_WC_Bundle_Retail_RQ']
# this includes merch and bundles. Fix
fo1['Revenue_Total_All'] = fo1['Revenue_Product_Net_Dct'] + fo1['Revenue_Gift_Wrap'] + fo1['Revenue_Shipping'] - fo1['Returns_in_Revenue']
# WooCom Royalty Qualified Quantity
# Only books will have a non-zero royalty rate
conditions = [
    fo1['Royalty_Rate_All'] > 0.0
    ]
choices = [ fo1['Quantity_Shipped'] - fo1['Quantity_Wholesale'] - fo1['Quantity_Returned'] ]
fo1['Royalty_Qualified_Quantity'] = np.select(conditions, choices, default=0)
# Only Books will have a non-zero royalty rate
conditions = [
    fo1['Royalty_Rate_All'] > 0.0
    ]
choices = [ fo1['Revenue_Total_Ind_Retail'] + fo1['Revenue_WC_Bundle_Retail'] - fo1['Returns_in_Revenue'] ]
fo1['Royalty_Qualified_Revenue'] = np.select(conditions, choices, default=0)
fo1['Royalty_Paid'] = fo1['Royalty_Qualified_Revenue'] * fo1['Royalty_Rate_All']

fo2 = fo1
fo2.isnull().sum(axis = 0)

conditions = [
   (fo2['Data_Source'] == 'WC'),
   (fo2['Data_Source'] == 'SCB Sales'),
   (fo2['Data_Source'] == 'SCB Returns')
    ]
choices = [fo2['WC_Quarter'], fo2['SCB_Sales_Qtr'], fo2['SCB_Return_Qtr'] ]
fo2['Combined_Quarter'] = np.select(conditions, choices, default = fo2['WC_Quarter'])
fo2['MKEY_Date'] = (pd.to_datetime(fo2['Year'].astype(str) + fo2['Month'], format='%Y%B'))
conditions = [
   (fo2['Data_Source'] == 'WC'),
   (fo2['Data_Source'] == 'SCB Sales'),
   (fo2['Data_Source'] == 'SCB Returns')
    ]
choices = [ fo2['MKEY_Date'] , fo2['MKEY_Date'] + pd.DateOffset(months=3), fo2['MKEY_Date'] ]
fo2['Combined_Month_SCB_fwd'] = np.select(conditions, choices, default=fo2['MKEY_Date'] )

fo2['Retail_Other_Sales'] = 0
fo2['Qty_Other_Sales'] = 0

fo2 = fo2.sort_values(by=['Order Number', 'ItemOrderSeq'], ascending=[False, False]).reset_index(drop=True)
fo2 = fo2.drop(['Royalty_Qualified_Quantity',
                'Royalty_Qualified_Revenue',
                'Royalty_Paid',
                'Revenue_Wholesale',
                'Revenue_WC_Ind_Retail'	,
                'Revenue_SCB_Ind_Retail',
                'Revenue_Total_Ind_Retail',
                'Revenue_WC_Bundle_Retail_RQ',
                'Revenue_WC_Bundle_Retail',
                'Revenue_SCB_Returns',
                'Revenue_WC_Royalty_Qualified',
                'Quantity_WC_Retail',
                'Quantity_SCB_Sales',
                'Quantity_SCB_Returns',
                'Bundle_Order_Line_Total',
                'Bundle_Order_Line_Subtotal',
                'Revenue_Wholesale_Order_Line_Total',
                'Revenue_Wholesale_Order_Line_Subtotal',
                'Royalty_Rate_All',
                'Retail_Other_Sales',
                'Qty_Other_Sales'
                ], axis='columns')

# Final Checks
fo2['Order Number'] = fo2['Order Number'].astype(str)
fo2['Coupon Amount'] = fo2['Coupon Amount'].astype(str)
#rename the column 'Country Name (Shipping)'
fo2 = fo2.rename(columns={'Country Name (Shipping)': 'ShippingCountry'})

# rename for BQ
fo2 = fo2.rename(columns={
    'Customer User Email' : 'Customer_User_Email',
    'Order Number' : 'Order_Number',
    'Customer User Email' : 'Customer_User_Email',
    'Customer Role' : 'Customer_Role',
    'Coupon Code' : 'Coupon_Code',
    'Coupon Type' : 'Coupon_Type',
    'Coupon Amount' : 'Coupon_Amount'
    })

# Save to Buckets and BigQuery
# CS
fact_order_blob = "fact_tables/FACT_Order.csv"
save_bucket(fo2, bucket_name, fact_order_blob, storage_client)
# BQ
fact_database = f"fact_tables"
orders_fact_table = f"FACT_Order"
save_to_bq(fo2, f"{project_id}.{fact_database}.{orders_fact_table}", bq_client)