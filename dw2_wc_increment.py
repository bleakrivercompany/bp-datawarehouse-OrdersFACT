# -*- coding: utf-8 -*-
# WOOCOM INCREMENTAL API CALL
import requests
import pandas as pd
import math
from flatten_json import flatten
import numpy as np
from datetime import datetime
from google.cloud import storage
import json
from google.oauth2 import service_account
# Custom Functions
from wc_block2_helpers import get_unique_indices_from_columns
from wc_block2_helpers import clean_text_column
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

""" 
PRODUCT DIM API CALL & TRANSFORM
"""
# Start the API Call
pg = ('https://buttonpoetry.com/wp-json/wc/v3/products?consumer_key=' + p_consumerkey + '&consumer_secret=' + p_consumersecret)

pg_max = requests.get(pg)
print(pg_max.headers)
total_prods = pg_max.headers.get('x-wp-total')

page_max = int(total_prods)

page_max = math.ceil(page_max/10)

print(page_max)

Products_Run = pd.DataFrame(columns= ['id'])

page_ct = 0

while page_ct < page_max:
    page_ct += 1
    # Using an f-string for cleaner URL building
    url = (
        f"https://buttonpoetry.com/wp-json/wc/v3/products?"
        f"consumer_key={p_consumerkey}&"
        f"consumer_secret={p_consumersecret}&"
        f"page={page_ct}&"
        f"per_page=10"
    )
    print(url)
    # It's good practice to use a different variable name for the response
    response_json = requests.get(url).json()
    
    url_flat = [flatten(d) for d in response_json]
    flat2 = pd.DataFrame(url_flat)
    Products_Run = pd.concat([Products_Run, flat2], ignore_index=True)

# Upload Full Product Query
destination_blob_name = "stage/dim_stage/Product_Full_WC_Query.csv"  # Desired filename in GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
# Upload the DataFrame as a CSV string
blob.upload_from_string(Products_Run.to_csv(index=False), content_type="text/csv")

#Cleanup for Final dimProduct
Products_Dim = Products_Run[['id', 'name', 'type', 'categories_0_id', 'categories_0_name', 'shipping_class' ]]

Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.strip())
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'\u201c', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'\u201d', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'&ndash; ', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u' <BR>&nbsp;<BR>', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'#038; ', ''))

conditions = [
    #Books
    (Products_Dim['categories_0_name'] == 'Books')  ,
    (Products_Dim['categories_0_name'] == 'Forthcoming Books'),
    (Products_Dim['categories_0_name'] == 'Out of Print'),
    (Products_Dim['categories_0_name'] == 'Audiobooks'),
    (Products_Dim['categories_0_name'] == 'E-Books') ,
    (Products_Dim['shipping_class'] == 'books'),
    #Bundles
    (Products_Dim['categories_0_name'] == 'Bundles')  ,
    (Products_Dim['name'].str.contains(r'Bundle')),
    (Products_Dim['shipping_class'] == 'bundles'),
    #Merch
    (Products_Dim['categories_0_name'] == 'Merch'),
    (Products_Dim['categories_0_name'] == 'Featured') ,
    (Products_Dim['shipping_class'] == 'clothing'),
    #Other
    (Products_Dim['categories_0_name'] == 'Workshop')
    ]
choices = ['Book', 'Book', 'Book', 'Book', 'Book', 'Book', 'Bundles', 'Bundles', 'Bundles', 'Merch', 'Merch', 'Merch', 'Workshop' ]

Products_Dim['Product_Category'] = np.select(conditions, choices, default = 'Check')

Products_Dim = Products_Dim.drop(columns=['categories_0_name', 'categories_0_id'])

# Upload Full Product Query
destination_blob_name = "dimension_tables/Product_Dim.csv"  # Desired filename in GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
# Upload the DataFrame as a CSV string
blob.upload_from_string(Products_Dim.to_csv(index=False), content_type="text/csv")
"""
WOOCOM API CALL BEGIN
"""
# API Config
api_url = 'https://buttonpoetry.com/wp-json/wc/v3/orders'
per_page = 100
days_to_fetch = 61

#Setup Dates
wc_start = datetime.now()

print("WooCom Scrape Begin: ", wc_start)

CDate = pd.to_datetime('today').date()
QRun_DT = CDate + pd.DateOffset(-days_to_fetch)
QRun = QRun_DT.date()

"""
RUN THE LAST 31 DAYS
"""
#Books['Quantity Received'] = Books['Quantity Received'].astype(int)
after = QRun.strftime("%Y-%m-%d")
before = CDate.strftime("%Y-%m-%d")

#Get All Orders

headers_params = {
        'consumer_key': p_consumerkey,
        'consumer_secret': p_consumersecret,
        'after': f'{after}T00:00:00',
        'before': f'{before}T23:59:59'
    }

# Initial request to get total pages from headers
response = requests.get(api_url, params=headers_params)
response.raise_for_status() # Raise an exception for bad status codes
order_total = int(response.headers.get('x-wp-total'))

print("WooCom Orders for", QRun, "to ", CDate, " : ", order_total)

page_max = math.ceil(order_total/100)

# 1. Initialize an empty Python LIST, not a DataFrame
# Pandas cannot use the APPEND function anymore
all_pages_data = []

page_ct = 0
while page_ct < page_max:
    page_ct += 1
    
    api_params = {
        'consumer_key': p_consumerkey,
        'consumer_secret': p_consumersecret,
        'after': f'{after}T00:00:00',
        'before': f'{before}T23:59:59',
        'per_page': 100,
        'page': page_ct  
    }
    
    # Fetch the data for the current page
    response_json = requests.get(api_url, params=api_params).json()

    # Flatten the JSON response
    flat_data = [flatten(d) for d in response_json]

    # 3. Append the flattened data to the list
    if flat_data:
        all_pages_data.extend(flat_data)

# 4. Create the final DataFrame ONCE after the loop is complete
Orders_Q_Run = pd.DataFrame(all_pages_data)

"""
~*~ BLOCK 2: ITEMS, COUPONS, REFUNDS, PRODUCTS
"""
OrdersDF = Orders_Q_Run.copy()
ref_array = get_unique_indices_from_columns(OrdersDF, 'refunds_')
coup_array = get_unique_indices_from_columns(OrdersDF, 'coupon_lines_')
item_indices = get_unique_indices_from_columns(OrdersDF, 'line_items_')

# Get the max item index for your loop
if item_indices.size > 0:
    max_items_page = item_indices.max()
else:
    max_items_page = -1 # Handles cases with no line items

# LOOP for items and metas   
all_item_dfs = []

for fcount in item_indices:
    # The item number is based on the index.
    item_num = fcount + 1

    # --- Error Handling for Optional Columns ---
    # Define potential metadata columns and add them to OrdersDF if they don't exist.
    meta_cols = [
        f'line_items_{fcount}_meta_data_0_value',
        f'line_items_{fcount}_meta_data_1_value',
        f'line_items_{fcount}_meta_data_1_key',
        f'line_items_{fcount}_meta_data_2_value',
        f'line_items_{fcount}_meta_data_2_key',
        f'line_items_{fcount}_composite_parent',
        f'line_items_{fcount}_bundled_by',
        f'line_items_{fcount}_bundled_item_title'
    ]
    for col in meta_cols:
        if col not in OrdersDF.columns:
            OrdersDF[col] = '' # or np.nan

    # 2. For each item index, create a new DataFrame from the relevant slices of the original data.
    DFS = pd.DataFrame({
        'OrderID': OrdersDF['id'],
        'ParentID': OrdersDF['parent_id'],
        'OrderStatus': OrdersDF['status'],
        'OrderKey': OrdersDF['order_key'],
        'OrderNumber': OrdersDF['number'],
        'OrderDate': OrdersDF['date_created'],
        'ModDate': OrdersDF['date_modified'],
        'CustomerID': OrdersDF['customer_id'],
        'CustomerEmail': OrdersDF['billing_email'],
        'DiscountTotal': OrdersDF['discount_total'],
        'DiscountTax': OrdersDF['discount_tax'],
        'ShippingTotal': OrdersDF['shipping_total'],
        'ShippingTax': OrdersDF['shipping_tax'],
        'CartTax': OrdersDF['cart_tax'],
        'OrderTotal': OrdersDF['total'],
        'TaxTotal': OrdersDF['total_tax'],
        'ShippingCity': OrdersDF['shipping_city'],
        'ShippingState': OrdersDF['shipping_state'],
        'ShippingZip': OrdersDF['shipping_postcode'],
        'ShippingCountry': OrdersDF['shipping_country'],
        'TransactionID': OrdersDF['transaction_id'],
        'DateCompleted': OrdersDF['date_completed'],
        'DatePaid': OrdersDF['date_paid'],
        # Items - dynamically select columns for the current fcount
        'ItemID': OrdersDF[f'line_items_{fcount}_id'],
        'ItemOrderSeq': item_num,
        'ItemName': OrdersDF[f'line_items_{fcount}_name'],
        'ItemSKU': OrdersDF[f'line_items_{fcount}_sku'],
        'ItemProductID': OrdersDF[f'line_items_{fcount}_product_id'],
        'ItemVarID': OrdersDF[f'line_items_{fcount}_variation_id'],
        'ItemQuantity': OrdersDF[f'line_items_{fcount}_quantity'],
        'ItemTaxClass': OrdersDF[f'line_items_{fcount}_tax_class'],
        'ItemPrice': OrdersDF[f'line_items_{fcount}_price'],
        'ItemSubTotal': OrdersDF[f'line_items_{fcount}_subtotal'],
        'ItemSubTotalTax': OrdersDF[f'line_items_{fcount}_subtotal_tax'],
        'ItemTotal': OrdersDF[f'line_items_{fcount}_total'],
        'ItemTotalTax': OrdersDF[f'line_items_{fcount}_total_tax'],
        'ItemMetaKey': OrdersDF[f'line_items_{fcount}_meta_data_2_key'],
        'ItemMetaValue': OrdersDF[f'line_items_{fcount}_meta_data_2_value'],
        'ItemBookType': OrdersDF[f'line_items_{fcount}_meta_data_0_value'],
        'ItemCompParent': OrdersDF[f'line_items_{fcount}_composite_parent'],
        'ItemBundleCode': OrdersDF[f'line_items_{fcount}_bundled_by'],
        'ItemBundleTitle': OrdersDF[f'line_items_{fcount}_bundled_item_title'],
        'ItemBundleByAlt': OrdersDF[f'line_items_{fcount}_meta_data_1_key'],
        'ItemBundleByTitleAlt': OrdersDF[f'line_items_{fcount}_meta_data_1_value'],
    })

    # 3. Add the newly created DataFrame to the list.
    all_item_dfs.append(DFS)

# 4. After the loop, concatenate all the DataFrames in the list into a single DataFrame.
if all_item_dfs:
    DFC = pd.concat(all_item_dfs, ignore_index=True)
else:
    # If no items were processed, create an empty DataFrame with the correct columns.
    DFC = pd.DataFrame(columns=['OrderID'])
    
DFC = DFC[DFC['ItemID'].notna()]

# Convert empty strings in both columns to NaN for reliable checking
# For ItemCompParent
DFC['ItemCompParent'] = DFC['ItemCompParent'].replace('', np.nan).astype(object)
# For ItemBundleCode
DFC['ItemBundleCode'] = DFC['ItemBundleCode'].replace('', np.nan).astype(object)
# For ItemBundleTitle
DFC['ItemBundleTitle'] = DFC['ItemBundleTitle'].replace('', np.nan).astype(object)

""" 
BLOCK 2 pt2: CLEAN UP AND CATEGORIZE
"""
# Two columns may have Gift Wrapped
DFC.loc[(DFC['ItemMetaKey'] == 'Gift Wrapped') | (DFC['ItemBundleByAlt'] == '_gift_wrap'), 'Giftwrapped'] = 'Yes'
# Two columns may have Wholesaler info
DFC.loc[(DFC['ItemMetaKey'] == '_wwp_wholesale_role') | (DFC['ItemBundleByAlt'] == '_wwp_wholesale_prices') | (DFC['ItemBundleByAlt'] == '_wwp_wholesale_role'), 'Wholesale'] = 'Yes'
# 1. Define the BundleID using OR logic (This part is fine)
# Note: Ensure you are using .str.contains() with case sensitivity you intend (default is case sensitive)
DFC.loc[
    (DFC['ItemName'].str.contains(r'Bundle', case=False) | 
     DFC['ItemName'].str.contains(r'Combo', case=False) | 
     (DFC['ItemBundleCode'].notnull()) | 
     (DFC['ItemCompParent'].notnull())), 
    'BundleID' 
] = DFC['ItemID']

# 2. Define the BundledBy column using np.where for precedence
# This sets BundledBy based on a single, non-overwriting conditional statement:
# IF ItemCompParent is not null, use it. 
# ELSE IF ItemBundleCode is not null, use it.
# ELSE, use NaN.

DFC['BundledBy'] = np.where(
    DFC['ItemCompParent'].notnull(), 
    DFC['ItemCompParent'], 
    np.where(
        DFC['ItemBundleCode'].notnull(),
        DFC['ItemBundleCode'],
        np.nan # Use Pandas' default null value
    )
)
# Clean Item Names
DFC['ItemName'] = clean_text_column(DFC['ItemName'])
# Dates
DFC['OrderDate'] = pd.to_datetime(DFC['OrderDate'])
DFC['ModDate'] = pd.to_datetime(DFC['ModDate'])
DFC['DateCompleted'] = pd.to_datetime(DFC['DateCompleted'])
DFC['DatePaid'] = pd.to_datetime(DFC['DatePaid'])

""" 
BLOCK 3: MERGE PRODUCT DIM
# Product Dim load

# OLD CODE
prod_blob_name = "dimension_tables/Product_Dim.csv"

prods_0 = get_bucket_csv(bucket_name, prod_blob_name).reset_index(drop=True)
prods = prods_0.astype({'id' : float ,
                             'name' : object ,
                             'type' : object ,
                             'Product_Category' : object 
                             } )
"""
prods = Products_Dim.copy()
# Merge prods to the DFC on Product ID to id
DFC = pd.merge(DFC, prods, left_on='ItemProductID', right_on='id', how='left')
# Drop id column and others
DFC = DFC.drop(columns=['ItemMetaKey', 'ItemMetaValue', 'ItemBundleByAlt', 'ItemBundleByTitleAlt', 'type', 'id', 'name'])

"""
COUPONS 
"""
all_coupons = []
coup_frame = (0-1)

# Coupon Lines are only 0 and 1

for coup in coup_array:
    coup_frame += 1
    item_num = coup_frame + 1

    ccount = str(coup_frame)
    item_ct_str = str(item_num)

    coup_df = pd.DataFrame({
        'OrderID' : OrdersDF['id'],
        'ItemOrderSeq' : item_ct_str,
        'CouponID' : OrdersDF['coupon_lines_'+ccount+'_id'],
        'CouponCode' : OrdersDF['coupon_lines_'+ccount+'_code'],
        'CouponDiscount' : OrdersDF['coupon_lines_'+ccount+'_discount'],
        'CouponDiscountTax' : OrdersDF['coupon_lines_'+ccount+'_discount_tax'],
        })
    all_coupons.append(coup_df)
if all_coupons:
    CPDB = pd.concat(all_coupons, ignore_index = True)
else:
    # If no items were processed, create an empty DataFrame with the correct columns.
    CPDB = pd.DataFrame(columns=['OrderID']) # Add other columns as needed for an empty case
CPDB = CPDB.loc[CPDB['CouponID'].notna()]

""" 
REFUNDS
"""
all_refunds = []
ref_frame = (0-1)

# Refunds is just 1 in this setup

for ref in ref_array:

    ref_frame += 1
    item_num = ref_frame + 1

    rcount = str(ref_frame)
    item_ct_str = str(item_num)

    ref_df = pd.DataFrame({
        'OrderID' : OrdersDF['id'],
        'ItemOrderSeq' : item_ct_str,
        'RefundID' : OrdersDF['refunds_'+rcount+'_id'],
        'RefundAmount' : OrdersDF['refunds_'+rcount+'_reason'],
        'RefundTotal' : OrdersDF['refunds_'+rcount+'_total'],
        })
    all_refunds.append(ref_df)

if all_refunds:
    RFDB = pd.concat(all_refunds, ignore_index = True)
else:
    # If no items were processed, create an empty DataFrame with the correct columns.
    RFDB = pd.DataFrame(columns=['OrderID']) # Add other columns as needed for an empty case
RFDB = RFDB.loc[RFDB['RefundID'].notna()]

# Merge Joinkey Cleanups
DFC['ItemOrderSeq'] = DFC['ItemOrderSeq'].astype(int)
RFDB['ItemOrderSeq'] = RFDB['ItemOrderSeq'].astype(int)
CPDB['ItemOrderSeq'] = CPDB['ItemOrderSeq'].astype(int)

# Coupon Types
CPDB['CouponDiscount'] = CPDB['CouponDiscount'].replace('',0).astype(float)
CPDB['CouponDiscountTax'] = CPDB['CouponDiscountTax'].replace('',0).astype(float)

# Refund Types
RFDB['RefundTotal'] = RFDB['RefundTotal'].replace('',0).astype(float)

# Items DType Changes
DFC['ItemTotal'] = DFC['ItemTotal'].replace('',0).astype(float)
DFC['ItemSubTotal'] = DFC['ItemSubTotal'].replace('',0).astype(float)
DFC['ItemTotalTax'] = DFC['ItemTotalTax'].replace('',0).astype(float)
DFC['ItemSubTotalTax'] = DFC['ItemSubTotalTax'].replace('',0).astype(float)
DFC['DiscountTotal'] = DFC['DiscountTotal'].replace('',0).astype(float)
DFC['DiscountTax'] = DFC['DiscountTax'].replace('',0).astype(float)
DFC['ShippingTotal'] = DFC['ShippingTotal'].replace('',0).astype(float)
DFC['ShippingTax'] = DFC['ShippingTax'].replace('',0).astype(float)
DFC['CartTax'] = DFC['CartTax'].replace('',0).astype(float)
DFC['OrderTotal'] = DFC['OrderTotal'].replace('',0).astype(float)
DFC['TaxTotal'] = DFC['TaxTotal'].replace('',0).astype(float)


""" 
BLOCK 4: THE BIG MERGE
"""
DFC['BundleID'] = [str(x).replace('.0','') for x in DFC['BundleID'] ]
DFC['BundledBy'] = [str(x).replace('.0','') for x in DFC['BundledBy'] ]
DFC['ItemID'] = [str(x).replace('.0','') for x in DFC['ItemID'] ]

pd_merge_order_coup = pd.merge(DFC, CPDB, on=['OrderID', 'ItemOrderSeq'],how='left')
pd_merge_order = pd.merge(pd_merge_order_coup, RFDB, on= ['OrderID', 'ItemOrderSeq'], how='left')
pd_merge_order['ItemOrderSeq'] = pd_merge_order['ItemOrderSeq'].astype(int)
pd_merge_order = pd_merge_order.sort_values(by=['OrderDate', 'OrderID', 'ItemOrderSeq'], ascending=[False, True, True])
pd_sum = (
    pd_merge_order.groupby('OrderNumber')
    .agg(
        TotalItems=('ItemQuantity', 'sum'),
        TotalProducts=('ItemProductID', 'nunique'),
        OrderSubTotal=('ItemSubTotal', 'sum')
    )
    .reset_index()
)
pd_merge_order['ItemDiscountAmount'] = pd_merge_order['ItemSubTotal'] - pd_merge_order['ItemTotal']
pd_merge_order['CityStateZip'] = pd_merge_order['ShippingCity']  + ', ' + pd_merge_order['ShippingState'] + " " + pd_merge_order['ShippingZip']
# 8/10/2025 add new Coupon Codes to Wholesale flag conditions. Have to add them after the Coupon Frame is created
pd_merge_order.loc[pd_merge_order['CouponCode'].isin(['WHOLESALE60', 'wholesale60', 'WHOLESALE40', 'wholesale40', 'WHOLESALE50', 'wholesale50']), 'Wholesale'] = 'Yes'
pd_merge_order['CustomerRole'] = pd_merge_order['Wholesale']
pd_merge_order['CustomerRole'] = pd_merge_order['CustomerRole'].where(pd_merge_order['CustomerRole'].isnull(), 'Wholesale Customer').fillna('Customer')
pd_merge_order['GiftWrapCost'] = ''
pd_merge_order['LinkedAuthor'] = ''
pd_merge_order['CouponType'] = ''
pd_merge_order['CouponAmount'] = ''
pd_merge_order['CustomerUserName'] = ''
pd_merge_order['CompositeID'] = ''
pd_merge_order['Component of'] = ''
pd_merge_orders = pd.merge(pd_merge_order, pd_sum, on= ['OrderNumber'], how='left').reset_index()
# Filter for only completed and processing orders
pd_merge_orders = pd_merge_orders[(pd_merge_orders.OrderStatus.isin(['completed', 'pre-ordered', 'processing']))]

# The complete rename map, which will be used for both selecting and renaming
column_rename_map = {
    # Order Information
    'OrderNumber'       : 'Order Number',
    'OrderStatus'       : 'Order Status',
    'OrderDate'         : 'Order Date',
    'TotalProducts'     : 'Total products',
    'TotalItems'        : 'Total items',
    'OrderSubTotal'     : 'Order Subtotal Amount',
    'ShippingTotal'     : 'Order Shipping Amount',
    'OrderTotal'        : 'Order Total Amount',

    # Customer Information
    'CustomerID'        : 'Customer User ID',
    'CustomerUserName'  : 'Customer Username',
    'CustomerEmail'     : 'Customer User Email',
    'CustomerRole'      : 'Customer Role',

    # Shipping Information
    'CityStateZip'      : 'City, State, Zip (Shipping)',
    'ShippingCountry'   : 'Country Name (Shipping)',
    'ShippingCity'      : 'ShippingCity',
    'ShippingState'     : 'ShippingState',
    'ShippingZip'       : 'ShippingZip',

    # Item & Product Details
    'BundleID'          : 'Bundle ID',
    'BundledBy'         : 'Bundled By',
    'CompositeID'       : 'Composite ID',
    'Component of'      : 'Component of',
    'ItemOrderSeq'      : 'ItemOrderSeq',
    'ItemName'          : 'Product Name',
    'ItemQuantity'      : 'Quantity',
    'ItemSubTotal'      : 'Order Line Subtotal',
    'ItemDiscountAmount': 'Item Discount Amount',
    'ItemTotal'         : 'Order Line Total',
    'Product_Category'  : 'Category',
    'ItemBookType'      : 'Subcategory',

    # Extras & Promotions
    'Giftwrapped'       : 'Gift Wrap',
    'GiftWrapCost'      : 'Gift Wrap Cost',
    'LinkedAuthor'      : 'Linked Author',
    'CouponCode'        : 'Coupon Code',
    'CouponType'        : 'Coupon Type',
    'CouponAmount'      : 'Coupon Amount',
    'CouponDiscount'    : 'Discount Amount',
}

# The single, chained operation
pd_merge_clean = (
    pd_merge_orders[list(column_rename_map.keys())]
    .rename(columns=column_rename_map)
)
# Final checks
pd_merge_clean['Customer Role'] = np.where(pd_merge_clean['Customer User Email'].str.contains('@buttonpoetry'), 'Administrator', pd_merge_clean['Customer Role'])
pd_merge_clean['Product Name'] = np.where(pd_merge_clean['Product Name'].str.contains('The Art of Taking the L Zine'), '“The Art of Taking the L” Zine Bundle', pd_merge_clean['Product Name'])
pd_merge_clean['Order Date'] = pd.to_datetime(pd_merge_clean['Order Date']).dt.date
# Save blob to GCP bucket
woocom_final = pd_merge_clean.drop_duplicates()
save_bucket(woocom_final, bucket_name, 'stage/woocom_stage/WooCom_Increment_Stage.csv')