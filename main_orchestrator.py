# -*- coding: utf-8 -*-
# BUTTON ETL
"""
This is the main ETL pipeline script

1. SCB runs on a separate VM designed for web scraping activities. ETL process assumes this is already done.
2. BP.com incremental API call and Product API call; data to staging and dimension
3. Merging of BP.com and SCB increments to complete data, cleaning, creation of staging tables for Books, Bundles, Merch
4. Creation of dimension tables for Books, Bundles, Merch
5. Orders and Royalties FACT tables created, Royality Report tables created

"""

import subprocess
import sys
import os

def run_script(script_name):
    """
    Runs a Python script using subprocess.
    """
    print(f"Running {script_name}...")
    try:
        # Use subprocess.run for simple execution
        subprocess.run([sys.executable, script_name], check=True)
        print(f"Successfully finished {script_name}.")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        # Exit the program if a script fails
        sys.exit(1)

def main():
    """
    The main orchestration function.
    """
    # Define the scripts to run in order
    scripts_to_run = [
        "dw2_wc_increment.py",
        "dw3_merge_inc.py",
        "dw4_book_dim.py",
        "dw5_order_royalty_fact.py"
    ]

    # Change to the directory containing the scripts
    # os.path.dirname is a safer way to get the directory of the current script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    for script in scripts_to_run:
        run_script(script)
    
    print("All scripts finished successfully!")

if __name__ == "__main__":
    main()