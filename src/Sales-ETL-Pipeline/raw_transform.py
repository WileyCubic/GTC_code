import pandas as pd
from Utils import logger, format_phone_number
import logging

logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")

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
        
        df['Paid at'] = pd.to_datetime(df['Paid at'], errors='coerce')
    
        df['Fulfilled at'] = pd.to_datetime(df['Fulfilled at'], errors='coerce')
        
        df['Created at'] = pd.to_datetime(df['Created at'], errors='raise')
        
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