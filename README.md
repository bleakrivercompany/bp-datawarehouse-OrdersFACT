# ETL Pipeline for Sales and Royalty Data Reporting

This repository contains a comprehensive ETL (Extract, Transform, Load) pipeline designed to process sales and order data from multiple sources (WooCommerce and SCB), generate standardized dimension tables, calculate author royalties, and produce final reporting tables. The pipeline leverages Python, Pandas, and Google Cloud Platform (GCP) services, including Google Cloud Storage (GCS) and BigQuery.

---

## 🚀 Key Features

### Data Integration and Transformation
*   **Multi-Source Integration:** Combines transactional data from WooCommerce (WC) and a distributor source (SCB).
*   **Incremental Data Management:** Scripts handle incremental API pulls for WooCommerce and merge them with existing complete order archives stored in GCS.
*   **Data Cleaning and Standardization:** Performs initial cleaning on source data, including parsing product names to identify book types. Helper utilities are available for cleaning text data.

### Dimension Table Generation
*   **Book Dimension (`Book_Dim`):** Created by reading master book information from BigQuery and matching it against cleaned source data using advanced techniques like **TF-IDF cosine similarity** for fuzzy matching titles. This dimension includes standardized royalty rates for different book types (Print, E-Book, Audiobook, Hardcover).
*   **Bundle and Merch Dimensions:** Generates `Bundle_Dim` and `Merch_Dim`. Bundle dimension logic includes matching source bundle names to master bundle data using fuzzy ratios.

### Fact Table and Reporting Generation
*   **FACT Royalty (`FACT_Royalty`):** An aggregated fact table designed specifically for royalty calculations. This table includes crucial revenue allocations:
    *   Handles complex **bundle revenue attribution** using predefined revenue share ratios.
    *   Excludes revenue and quantity associated with Wholesale Customers.
    *   Calculates the final `Royalty_Paid` amount based on `Royalty_Qualified_Revenue` and the book's `Royalty_Rate_All`.
*   **FACT Order (`FACT_Order`):** A detailed fact table maintaining granularity at the order line item level for all WC and SCB transactions.
*   **Reporting Outputs:** Generates summarized reports (`Royalty_Summary_Report_Complete`) and detailed printable reports, including specialized logic for **split royalties** between co-authors (e.g., Erkinnen & Hawley, Tyabji & Neal).

### GCP Utilities
*   Standardized helper functions handle core interactions with GCP services:
    *   Retrieving secrets from Secret Manager (`get_gcp_secret`).
    *   Reading and writing DataFrames to GCS buckets (`get_bucket_csv`, `save_bucket`).
    *   Loading DataFrames into BigQuery tables (`save_to_bq`).

---

## ⚙️ Prerequisites and Setup

This pipeline is built on the Google Cloud Platform (GCP) ecosystem and requires proper setup of services and credentials.

### 1. GCP Project and Services
*   **Project ID:** Scripts reference a project ID (e.g., `button-datawarehouse`).
*   **GCS Bucket:** A designated bucket is used for staging intermediate data and storing final CSV outputs (e.g., `cs-royalties-test`).
*   **BigQuery:** Used to store master dimension data (e.g., `books_info_source`, `bundle_info_source`) and to load the final fact and dimension tables.

### 2. Service Account Credentials
A GCP Service Account (SA) is used for authentication. The SA key must be stored in Secret Manager.

*   **Secret Manager Setup:** Requires secrets for WooCommerce API access (`wc_consumer_key`, `wc_consumer_secret`) and the SA key itself (`storage_sa_key`).
*   **Required Scopes:** The SA must be configured with broad necessary scopes, including `https://www.googleapis.com/auth/bigquery` and `https://www.googleapis.com/auth/cloud-platform`.

### 3. Dependencies
The Python environment requires several external libraries, including standard data science tools and GCP specific modules:
*   `pandas`
*   `numpy`
*   `google-cloud-bigquery`
*   `google-cloud-storage`
*   `google-cloud-secretmanager`
*   `fuzzywuzzy`
*   `sklearn.feature_extraction.text` (for TFIDF)

---

## 📂 Script Components Overview

| Script File | Primary Role in Pipeline | Key Function/Process |
| :--- | :--- | :--- |
| **dw2_wc_increment.py** | **Staging/Cleaning Layer:** Calls the WooCommerce API incrementally to pull order data, flattens the resulting JSON, transforms columns, and stages the data (`WooCom_Increment_Stage.csv`). | Handles dynamic parsing of nested JSON fields like `line_items`. |
| **dw3_merge_increment.py** | **Staging/Cleaning Layer:** Merges incremental WC/SCB data with archives. Cleans source product names to create intermediate book dimension files (`WooCom_Books_All.csv`, `SCB_Books_All.csv`) used in the Dimension ETL. | Handles logic for identifying book types (Print, E-Book) within source titles. 
| **dw4_book_dim.py** | **Dimension Layer:** Reads master data and staged clean data, performs fuzzy matching, and generates final `Book_Dim`, `Bundle_Dim`, and `Merch_Dim` tables. | Title matching using **TF-IDF similarity**. Saves to GCS and BigQuery. |
| **dw5_order_royalty_fact.py** | **Fact & Reporting Layer:** Reads dimensions and complete orders, calculates revenue splits, determines royalty qualified revenue, calculates `Royalty_Paid`, and generates `FACT_Royalty`, `FACT_Order`, and all final reports. | Bundle/Wholesale allocation logic. Generates split royalty reports. |