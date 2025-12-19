import pandas as pd
from utils import logger
import logging
import numpy as np
import datetime
from pathlib import Path



# imports used to test processing functions
from dotenv import load_dotenv
import os
ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(ENV_PATH)



pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

square_data = pd.read_csv(os.getenv('square_raw_data'))
shopify_data = pd.read_csv(os.getenv('shopify_raw_data'))

logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")



#----------------------------------------#
# ADDITIONAL UTILS FUNCTIONS
#----------------------------------------#

def format_phone_number(phone, logger):
    if pd.isna(phone):
        return 0
    phone = str(int(float(phone))) # Convert to string and remove decimal if present
    try:
        if len(phone) == 10:
            return f'({phone[:3]})-{phone[3:6]}-{phone[6:]}'
        elif len(phone) == 11:
            return f'{phone[0]}-({phone[1:4]}) {phone[4:7]}-{phone[7:]}'  
        elif len(phone) == 9:
            return f'({phone[:2]}) {phone[2:5]}-{phone[5:]}'
        elif len(phone) == 12:
            return f'{phone[0:2]}-({phone[2:5]}) {phone[5:8]}-{phone[8:]}'
        else:
            return ValueError
    except Exception as e:
        logger.error(f"Error formatting phone number {phone}: {e}")
        return phone



def transform_square(df):
    # fill na values
    
    df['Order ID'] = df.index + 1 # this is not in the first column of the dataframe
    
    try:                # convert is working
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='raise')
        logger.info('no errors found in order_date conversion')
    except Exception as e:
        logger.error(f'converting order_date: {e}')
        
    df['Order Subtotal'] = df['Order Subtotal'].astype(float).round(2)
    
    df['Order Shipping Price'] = df['Order Shipping Price'].astype(float).round(2).fillna(0)
    
    df['Order Tax Total'] = df['Order Tax Total'].astype(float).round(2).fillna(0)
    
    df['Order Total'] = df['Order Total'].astype(float).round(2)
    
    df['Order Refunded Amount'] = df['Order Refunded Amount'].astype(float).round(2).fillna(0)
    
    try:                # convert is working
        df['Fulfillment Date'] = pd.to_datetime(df['Fulfillment Date'], errors='raise')
        logger.info('no errors found in fulfillment_date conversion')
    except Exception as e:
        logger.error(f'converting fulfillment_date: {e}')
        
    df['Recipient Phone'] = df['Recipient Phone'].apply(format_phone_number).fillna(0)
    
    df['Item Price'] = df['Item Price'].astype(float).round(2)
    
    df['Item Options Total Price'] = df['Item Options Total Price'].astype(float).round(2)
    
    df['Item Total Price'] = df['Item Total Price'].astype(float).round(2)
    
    # Sort by Order Date and reset index
    df.sort_values(by='Order Date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    logger.info('Square data transformation complete')

    return df

square_data.head(50)
square_data.info()

blb = square_data.fillna(0).head()
blb.stack()

shopify_data.info()
blb = shopify_data.fillna(0).head()
blb.stack()












def transform_shopify(df):
    try: 
        # dropping cancelled orders and false orders
        df_mask = df['Cancelled at'].isna()
        df = df.loc[df_mask].copy()
        
        # remove unneeded apostrophes 
        
        df['Shipping Zip'] = df['Shipping Zip'].str.replace("'", "", regex=False)
        df['Billing Zip'] = df['Billing Zip'].str.replace("'", "", regex=False)
        
        # fill na values
        df.fillna({
                    'Notes': 'No Notes Given',
                    'Note Attributes': 'No Note Attributes Given',
                    'Billing Company': 'No Company Given',
                    'Shipping Company': 'No Company Given',
                    'Email': 'No Email Given',
                    }, inplace=True)        
        
        # fill na values in with values given under the same order id but in other rows
        
        df['Financial Status'] = df.groupby('Name')['Financial Status'].ffill().bfill()
        df['Paid at'] = df.groupby('Name')['Paid at'].ffill().bfill()
        df['Fulfillment Status'] = df.groupby('Name')['Fulfillment Status'].ffill().bfill()
        df['Fulfilled at'] = df.groupby('Name')['Fulfilled at'].ffill().bfill()
        
        df['Billing Name'] = df.groupby('Name')['Billing Name'].ffill().bfill()
        df['Billing Street'] = df.groupby('Name')['Billing Street'].ffill().bfill()
        df['Billing Address1'] = df.groupby('Name')['Billing Address1'].ffill().bfill()
        df['Billing City'] = df.groupby('Name')['Billing City'].ffill().bfill()
        df['Billing Zip'] = df.groupby('Name')['Billing Zip'].ffill().bfill()
        df['Billing Province'] = df.groupby('Name')['Billing Province'].ffill().bfill()
        df['Billing Country'] = df.groupby('Name')['Billing Country'].ffill().bfill()
        
        df['Shipping Name'] = df.groupby('Name')['Shipping Name'].ffill().bfill()
        df['Shipping Street'] = df.groupby('Name')['Shipping Street'].ffill().bfill()
        df['Shipping Address1'] = df.groupby('Name')['Shipping Address1'].ffill().bfill()
        df['Shipping City'] = df.groupby('Name')['Shipping City'].ffill().bfill()
        df['Shipping Zip'] = df.groupby('Name')['Shipping Zip'].ffill().bfill()
        df['Shipping Province'] = df.groupby('Name')['Shipping Province'].ffill().bfill()
        df['Shipping Country'] = df.groupby('Name')['Shipping Country'].ffill().bfill()
        
        df['Payment Reference'] = df.groupby('Name')['Payment Reference'].ffill().bfill()
        df['Id'] = df.groupby('Name')['Id'].ffill().bfill()
        df['Payment ID'] = df.groupby('Name')['Payment ID'].ffill().bfill()
        df['Payment Reference'] = df.groupby('Name')['Payment Reference'].ffill().bfill()

        # convert date columns
        
        df['Paid at'] = df['Paid at'].apply(datetime.datetime.date)
    
        df['Fulfilled at'] = df['Fulfilled at'].apply(datetime.datetime.date)
        
        df['Created at'] = df['Created at'].apply(datetime.datetime.date)
        
        # format phone numbers
        
        df['Billing Phone'] = df['Billing Phone'].apply(format_phone_number).fillna(0)
        
        df['Shipping Phone'] = df['Shipping Phone'].apply(format_phone_number).fillna(0)
        
        df['Phone'] = df['Phone'].apply(format_phone_number).fillna(0)
        
        #rounding floats to 2 decimal
        
        df['Subtotal'] = df['Subtotal'].astype(float).round(2)
        
        df['Shipping'] = df['Shipping'].astype(float).round(2)
        
        df['Taxes'] = df['Taxes'].astype(float).round(2)
        
        df['Total'] = df['Total'].astype(float).round(2)
        
        df['Lineitem price'] = df['Lineitem price'].astype(float).round(2)

        # Formating tax columns
        
        df.fillna({
            'Tax 1 Name': 'No Name Given',
            'Tax 1 Value': 0, 
            'Tax 2 Name': 'No Name Given',
            'Tax 2 Value': 0,
            'Tax 3 Name': 'No Name Given',
            'Tax 3 Value': 0,
            'Tax 4 Name': 'No Name Given',
            'Tax 4 Value': 0,
            'Tax 5 Name': 'No Name Given',
            'Tax 5 Value': 0
        }, inplace=True)
        df['Tax 1 Value'] = df['Tax 1 Value'].astype(float).round(2)
        df['Tax 2 Value'] = df['Tax 2 Value'].astype(float).round(2)
        df['Tax 3 Value'] = df['Tax 3 Value'].astype(float).round(2)
        df['Tax 4 Value'] = df['Tax 4 Value'].astype(float).round(2)
        df['Tax 5 Value'] = df['Tax 5 Value'].astype(float).round(2)     
        
    except Exception as e:
        logger.error(f'converting: {e}')
    
    
    # Sort by Order Date and reset index
    df.sort_values(by='Name', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    logger.info('Shopify data transformation complete')
    
    return df