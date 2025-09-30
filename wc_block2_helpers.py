import pandas as pd
import numpy as np
import math

def get_unique_indices_from_columns(dataframe: pd.DataFrame, column_pattern: str) -> np.ndarray:
    """
    Scans DataFrame columns for a specific pattern, extracts numerical indices,
    and returns a sorted array of unique integers.

    This is useful for finding all existing indices from flattened JSON columns
    like 'line_items_0_name', 'line_items_1_name', etc.

    Args:
        dataframe (pd.DataFrame): The DataFrame to scan.
        column_pattern (str): The text pattern to search for in the column names
                              (e.g., 'refunds_', 'line_items_').

    Returns:
        np.ndarray: A sorted numpy array of the unique integer indices found.
                    Returns an empty array if no matches are found.
    """
    # Use a pandas Series for efficient string operations
    all_columns = pd.Series(dataframe.columns)

    # 1. Filter columns that contain the specified pattern
    relevant_columns = all_columns[all_columns.str.contains(column_pattern, na=False)]

    if relevant_columns.empty:
        return np.array([], dtype=int)

    # 2. Extract the first sequence of digits from each column name
    # .squeeze() converts the resulting single-column DataFrame to a Series
    indices = relevant_columns.str.extract(r'(\d+)', expand=False).squeeze()
    
    # FIX: If squeeze() results in a single string, convert it back to a Series
    # This handles the case where only one column matches the pattern.
    if isinstance(indices, str):
        indices = pd.Series([indices])

    # 3. Clean, convert to numeric, and get unique sorted values
    # Drop any NaNs that might result from non-matching patterns or errors
    valid_indices = indices.dropna()
    
    # Convert to numeric, coercing any errors into NaT (which are then dropped)
    numeric_indices = pd.to_numeric(valid_indices, errors='coerce').dropna()

    # Get the unique integer values and sort them
    unique_sorted_indices = np.sort(numeric_indices.unique()).astype(int)

    return unique_sorted_indices

# --- EXAMPLE USAGE ---
# This demonstrates how to replace your original script's logic

# Assume 'OrdersDF' is your DataFrame loaded from the previous step
# OrdersDF = pd.read_csv('path_to_your_data.csv') # Example of loading data

# Create a dummy DataFrame for demonstration if OrdersDF doesn't exist
try:
    OrdersDF
except NameError:
    print("Creating a dummy DataFrame for demonstration purposes.")
    dummy_data = {
        'id': [1, 2],
        'line_items_0_name': ['Book', 'Mug'],
        'line_items_0_price': [15, 10],
        'line_items_1_name': ['Shirt', None],
        'line_items_1_price': [20, None],
        'coupon_lines_0_code': ['SALE10', None],
        'refunds_0_amount': [None, 5.0]
    }
    OrdersDF = pd.DataFrame(dummy_data)
    print("Dummy DataFrame created.")


print("\n--- Using the new function ---")

# Replace the repetitive blocks with clean function calls
ref_array = get_unique_indices_from_columns(OrdersDF, 'refunds_')
coup_array = get_unique_indices_from_columns(OrdersDF, 'coupon_lines_')
item_indices = get_unique_indices_from_columns(OrdersDF, 'line_items_')

print(f"Refund indices found: {ref_array}")
print(f"Coupon indices found: {coup_array}")
print(f"Line Item indices found: {item_indices}")

# You can now easily get the maximum value for your item loop
if item_indices.size > 0:
    # .max() gets the highest index value. Your loop seems to iterate up to this number.
    max_items_page = item_indices.max()
else:
    # If no items are found, set to -1 so your loop `while framecount < max_items_page` doesn't run
    max_items_page = -1

# The original script's `math.ceil` was redundant as the max index is already an integer.
# The loop logic `framecount = -1; while framecount < max_items_page` is a bit unusual.
# A more common Python pattern would be `for fcount in range(max_items_page + 1):`
# However, this calculation matches your original script's behavior.
print(f"\nCalculated 'max_items_page' for the loop: {max_items_page}")

def clean_text_column(text_series: pd.Series) -> pd.Series:
    """
    Cleans a Series of text data by stripping whitespace and removing
    a predefined set of special characters and HTML remnants.

    Args:
        text_series (pd.Series): The pandas Series containing the text to be cleaned.

    Returns:
        pd.Series: The cleaned pandas Series.
    """
    if not isinstance(text_series, pd.Series):
        raise TypeError("Input must be a pandas Series.")

    # Chain string operations for better performance and readability
    cleaned_series = (
        text_series.astype(str)
        .str.strip()
        .str.replace(r'[^a-zA-Z0-9\s]', ' ', regex=True)
        .str.replace(u'\u201c', '', regex=False)      # Left double quote
        .str.replace(u'\u201d', '', regex=False)      # Right double quote
        .str.replace('&ndash; ', '', regex=False)     # en dash
        .str.replace(' <BR>&nbsp;<BR>', '', regex=False) # HTML line breaks
        .str.replace('#038; ', '', regex=False)      # HTML ampersand entity
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
    return cleaned_series

def clean_hidden_chars(df):
    """
    Cleans hidden characters and normalizes whitespace in all object columns
    of a DataFrame intended to be treated as strings.
    """
    # Define common hidden/non-standard characters to replace
    # \xa0 is non-breaking space (common web artifact)
    # \x00 is the null byte (common database/file issue)
    hidden_chars_pattern = r'[\r\n\t\xa0\x00]'
    
    # 1. Identify all 'object' columns
    object_cols = df.select_dtypes(include=['object']).columns

    for col in object_cols:
        # Step 1: Replace non-visible characters with a single space
        # We replace with a space to prevent merging words together
        df[col] = df[col].astype(str).str.replace(
            hidden_chars_pattern, ' ', regex=True
        )
        
        # Step 2: Replace multiple spaces (including newlines/tabs converted to spaces) 
        # with a single space and remove leading/trailing whitespace.
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # Step 3: Handle empty strings created by cleaning (convert to NaN)
        df[col] = df[col].replace('', np.nan)

    return df

# Example Usage:
# DFC = clean_hidden_chars(DFC)