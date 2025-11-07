from Utils import log
import numpy as np
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os
import glob
import re
from ETL_for_CSV_data import get_input_csv_files, csv_to_dataframe





def get_input_csv_files() -> list:
    csv_input_folder = os.getenv('ETL_to_table_daily_csv_input')
    csv_files = glob.glob(csv_input_folder + '/*.csv')
    return csv_files

def csv_to_dataframe(files) -> pd.DataFrame:
    
    # identifying the source of files
    square_csv_pattern = r'.*orders-.*'
    square_csv_files = [file for file in files if re.search(square_csv_pattern, file)]
    shopify_csv_pattern = r'orders_.*'
    shopify_csv_files = [file for file in files if re.search(shopify_csv_pattern, file)]
    
        #processing the files into a single dataframe
        #use error handeling to catch any errors
    try:
        if len(square_csv_files) > 0:
            
            # processing the square files
            square_df = pd.concat([pd.read_csv(f) for f in square_csv_files], ignore_index=True)
            return square_df
        
        if len(shopify_csv_files) > 0:
                
            # processing the shopify files
            shopify_df = pd.concat([pd.read_csv(f) for f in shopify_csv_files], ignore_index=True)
            return shopify_df
        
        if len(square_csv_files) == 0 and len(shopify_csv_files) == 0:
            
            return ImportError
        
        if len(square_csv_files) > 0 and len(shopify_csv_files) > 0:
            
            return ImportError
        
    except Exception as e:
        
        return e
    
    
files = get_input_csv_files()
input_df = csv_to_dataframe(files)

print(input_df.info())

needed_mask = input_df[['Lineitem name', 'Shipping Name', 'Lineitem quantity', 'Lineitem price']]
print(needed_mask.info())