from Utils import logger
import logging
import pandas as pd
import os
import glob
import numpy as np
import re
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
logger.info("ETL_for_CSV_data module loaded.")

#------------------------------#
# Extracting data from csv files
#------------------------------#

def get_input_csv_files(csv_input_folder) -> list:
    csv_files = glob.glob(csv_input_folder + '/*.csv')
    return csv_files

#------------------------------#
# Function to process input csv files into dataframe
#------------------------------#

# this will only work if all the csv files come from the same source
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
            logger.info(f'square csv files found: {len(square_csv_files)}')
            # processing the square files
            square_df = pd.concat([pd.read_csv(f) for f in square_csv_files], ignore_index=True)
            return square_df
        
        if len(shopify_csv_files) > 0:
            logger.info(f'Shopify csv files found: {len(shopify_csv_files)}')
            # processing the shopify files
            shopify_df = pd.concat([pd.read_csv(f) for f in shopify_csv_files], ignore_index=True)
            return shopify_df
        
        if len(square_csv_files) == 0 and len(shopify_csv_files) == 0:
            logger.error('No recognizable csv files found')
            return ImportError
        
        if len(square_csv_files) > 0 and len(shopify_csv_files) > 0:
            logger.warning('Both square and shopify csv files found')
            return ImportError
        
    except Exception as e:
        logger.error(f'csv_to_dataframe function: {e}')
        return e

#------------------------------#   
# Function to clean the input data
#------------------------------#

def clean_input_data(df) -> pd.DataFrame:
    
    
    #cleaning square data
    if len(df.columns) == 32:
        logger.info(f'square database detected | Dataframe length: {len(df)}')
        
        #drop unneeded columns
        df1 = df[['Order Name', 'Item Quantity', 'Item Name', 'Item Variation', 'Item Modifiers', 'Item Price']]
        
        
        #formating columns 
        df1['Item Quantity'] = df1['Item Quantity'].astype(int)
        
        df1 = df1.replace({np.nan: 'None'})
        
        logger.info(f'square dataframe cleaned | Dataframe length: {len(df1)}')
        return df1
    
    
    if len(df.columns) == 79:
        logger.info('shopify database detected')
        
        #fill missing customer names
        
        df['Shipping Name'] = df.groupby('Name')['Shipping Name'].ffill().bfill()
        
        #drop unneeded columns
        df1 = df[['Lineitem quantity', 'Lineitem name', 'Lineitem price', 'Shipping Name']]
    
        #formatting columns
        df1 = df1.replace({np.nan: 'None'})
        logger.info(f'shopify dataframe cleaned | Dataframe length: {len(df1)}')
        return df1
    
    else:
        logger.warning('unrecognizable database detected')
        return None