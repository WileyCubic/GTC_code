from Utils import log
import pandas as pd
import os
import glob
import numpy as np
import re
from dotenv import load_dotenv
load_dotenv()

#------------------------------#
# Extracting data from csv files
#------------------------------#

# Path to input folder

def get_input_csv_files() -> list:
    csv_input_folder = os.getenv('ETL_to_table_daily_csv_input')
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
            log(f'square csv files found: {len(square_csv_files)}')
            # processing the square files
            square_df = pd.concat([pd.read_csv(f) for f in square_csv_files], ignore_index=True)
            return square_df
        
        if len(shopify_csv_files) > 0:
            log(f'Shopify csv files found: {len(shopify_csv_files)}')    
            # processing the shopify files
            shopify_df = pd.concat([pd.read_csv(f) for f in shopify_csv_files], ignore_index=True)
            return shopify_df
        
        if len(square_csv_files) == 0 and len(shopify_csv_files) == 0:
            log('No recognizable csv files found')
            return ImportError
        
        if len(square_csv_files) > 0 and len(shopify_csv_files) > 0:
            log('Both square and shopify csv files found')
            return ImportError
        
    except Exception as e:
        log(f'ERROR: in csv_to_dataframe function: {e}')
        return e

#------------------------------#   
# Function to clean the input data
#------------------------------#

def clean_input_data(df) -> pd.DataFrame:
    
    # Attribute lists related to input source

    square_attribute_list = [
        'Order',
        'Order Name',
        'Order Date',
        'Currency',
        'Order Subtotal', 
        'Order Shipping Price',
        'Order Tax Total',
        'Order Total',
        'Order Refunded Amount',
        'Fulfillment Date',
        'Fulfillment Type',
        'Fulfillment Status',
        'Channels',
        'Fulfillment Location',
        'Fulfillment Notes',
        'Recipient Name',
        'Recipient Email',
        'Recipient Phone',
        'Recipient Address',
        'Recipient Address 2',
        'Recipient Postal Code',
        'Recipient City',
        'Recipient Region',
        'Recipient Country',
        'Item Quantity',
        'Item Name',
        'Item SKU',
        'Item Variation',
        'Item Modifiers',
        'Item Price',
        'Item Options Total Price',
        'Item Total Price'
    ]

    shopify_attribute_list = [
        'Name',
        'Email',
        'Financial Status',
        'Paid at',
        'Fulfillment Status',
        'Fulfilled at',
        'Accepts Marketing',
        'Currency',
        'Subtotal',
        'Shipping',
        'Taxes',
        'Total',
        'Discount Code',
        'Discount Amount',
        'Shipping Method',
        'Created at',
        'Lineitem quantity',
        'Lineitem name',
        'Lineitem price',
        'Lineitem compare at price',
        'Lineitem sku',
        'Lineitem requires shipping',
        'Lineitem taxable',
        'Lineitem fulfillment status',
        'Billing Name',
        'Billing Street',
        'Billing Address1',
        'Billing Address2',
        'Billing Company',
        'Billing City',
        'Billing Zip',
        'Billing Province',
        'Billing Country',
        'Billing Phone',
        'Shipping Name',
        'Shipping Street',
        'Shipping Address1',
        'Shipping Address2',
        'Shipping Company',
        'Shipping City',
        'Shipping Zip',
        'Shipping Province',
        'Shipping Country',
        'Shipping Phone',
        'Notes',
        'Note Attributes',
        'Cancelled at',
        'Payment Method',
        'Payment Reference',
        'Refunded Amount',
        'Vendor',
        'Outstanding Balance',
        'Employee',
        'Location',
        'Device ID',
        'Id',
        'Tags',
        'Risk Level',
        'Source',
        'Lineitem discount',
        'Tax 1 Name',
        'Tax 1 Value',
        'Tax 2 Name',
        'Tax 2 Value',
        'Tax 3 Name',
        'Tax 3 Value',
        'Tax 4 Name',
        'Tax 4 Value',
        'Tax 5 Name',
        'Tax 5 Value',
        'Phone',
        'Receipt Number',
        'Duties',
        'Billing Province Name',
        'Shipping Province Name',
        'Payment ID',
        'Payment Terms Name',
        'Next Payment Due At',
        'Payment References'
    ]
    
    #cleaning square data
    if len(square_attribute_list) == len(df.columns):
        log('square database detected')
        
        #drop unneeded columns
        df = df.drop(columns=['Order',
        'Order Date',
        'Currency',
        'Order Subtotal', 
        'Order Shipping Price',
        'Order Tax Total',
        'Order Total',
        'Order Refunded Amount',
        'Fulfillment Date',
        'Fulfillment Type',
        'Fulfillment Status',
        'Channels',
        'Fulfillment Location',
        'Fulfillment Notes',
        'Recipient Name',
        'Recipient Email',
        'Recipient Phone',
        'Recipient Address',
        'Recipient Address 2',
        'Recipient Postal Code',
        'Recipient City',
        'Recipient Region',
        'Recipient Country',
        'Item SKU',
        'Item Options Total Price',
        'Item Total Price'
        ])
        
        #formating columns 
        df['Item Quantity'] = df['Item Quantity'].astype(int)
        df = df.replace({np.nan: 'None'})
        log('square dataframe cleaned')
        return df
    
    
    if len(shopify_attribute_list) == len(df.columns):
        log('shopify database detected')
        
        #fill missing customer names
        
        df['Shipping Name'] = df.groupby('Name')['Shipping Name'].ffill().bfill()
        
        #drop unneeded columns
        df = df.drop(columns=['Name',
        'Email',
        'Financial Status',
        'Paid at',
        'Fulfillment Status',
        'Fulfilled at',
        'Accepts Marketing',
        'Currency',
        'Subtotal',
        'Shipping',
        'Taxes',
        'Total',
        'Discount Code',
        'Discount Amount',
        'Shipping Method',
        'Created at',
        'Lineitem compare at price',
        'Lineitem sku',
        'Lineitem requires shipping',
        'Lineitem taxable',
        'Lineitem fulfillment status',
        'Billing Name',
        'Billing Street',
        'Billing Address1',
        'Billing Address2',
        'Billing Company',
        'Billing City',
        'Billing Zip',
        'Billing Province',
        'Billing Country',
        'Billing Phone',
        'Shipping Street',
        'Shipping Address1',
        'Shipping Address2',
        'Shipping Company',
        'Shipping City',
        'Shipping Zip',
        'Shipping Province',
        'Shipping Country',
        'Shipping Phone',
        'Notes',
        'Note Attributes',
        'Cancelled at',
        'Payment Method',
        'Payment Reference',
        'Refunded Amount',
        'Vendor',
        'Outstanding Balance',
        'Employee',
        'Location',
        'Device ID',
        'Id',
        'Tags',
        'Risk Level',
        'Source',
        'Lineitem discount',
        'Tax 1 Name',
        'Tax 1 Value',
        'Tax 2 Name',
        'Tax 2 Value',
        'Tax 3 Name',
        'Tax 3 Value',
        'Tax 4 Name',
        'Tax 4 Value',
        'Tax 5 Name',
        'Tax 5 Value',
        'Phone',
        'Receipt Number',
        'Duties',
        'Billing Province Name',
        'Shipping Province Name',
        'Payment ID',
        'Payment Terms Name',
        'Next Payment Due At',
        'Payment References'
        ])
    
        #formatting columns
        df = df.replace({np.nan: 'None'})
        log('shopify dataframe cleaned')
        return df
    
    else:
        log('unrecognizable database detected')
        return None