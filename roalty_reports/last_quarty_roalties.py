from dataclasses import dataclass
from pathlib import Path
import os
import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#

@dataclass
class SquarePathsConfig:
    csv_path: Path = Path(r'C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\data\royalty report copys\Affinitu_Q3_2026\orders-2026-07-01-2026-09-30.csv')

@dataclass
class ShopifyPathsConfig:
    csv_path: Path = Path(r'C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\data\royalty report copys\Affinitu_Q3_2026\orders_export_1 (1).csv')

@dataclass
class OutputDirConfig:
    main_output_dir: Path = Path(r'C:\Users\wiley\Desktop\D3 Design Inc\Royalties Reports')
    output_dir: Path = Path(r'C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\scripts\scripts output\Royalties Roports')

#----------------------------------------#
# Functions to load configurations
#----------------------------------------#

def path_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()

    return square_paths, shopify_paths


#----------------------------------------#
# EXTRACTION CSV FUNCTIONS
#----------------------------------------#

# Create DataFrame from a CSV file

def csv_to_dataframe(csv_path: Path) -> pd.DataFrame:
    print(f'Reading CSV file: {csv_path}')
    return pd.read_csv(csv_path)


#----------------------------------------#
# TRANSFORMATION FUNCTIONS
#----------------------------------------#

def transform_square(df):
    # fill na values

    square_slice = [
    'Order Date',
    'Order',
    'Item Name',
    'Order Name',
    'Recipient Country',
    'Recipient Postal Code',
    'Item Quantity',
    'Item Price',
    ]

    df = df[square_slice].copy()

    try:                # convert is working
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='raise')
        df['Order Date'] = df['Order Date'].dt.strftime('%Y-%m-%d')
        print('no errors found in order_date conversion')
    except Exception as e:
        print(f'converting order_date: {e}')
        
    df['Item Price'] = df['Item Price'].astype(float).round(2)

    print('Square data transformation complete')

    return df


def transform_shopify(df):
    
    try:
        # dropping cancelled orders and false orders
        df_mask = df['Cancelled at'].isna()
        df = df.loc[df_mask].copy()
    except Exception as e:
        print(f'Error dropping cancelled orders: {e}')
    
    
    
    shopify_slice = [
    'Paid at',
    'Name',
    'Lineitem name',
    'Shipping Name',
    'Shipping Country',
    'Shipping Zip',
    'Lineitem quantity',
    'Lineitem price',
    ]

    df = df[shopify_slice].copy()
    
    try:

        df['Paid at'] = pd.to_datetime(df['Paid at'], errors='raise')
        df['Paid at'] = df['Paid at'].dt.strftime('%Y-%m-%d')
        # remove unneeded apostrophes

        df['Shipping Zip'] = df['Shipping Zip'].str.replace("'", "", regex=False)

        # fill na values in with values given under the same order id but in other rows
        df['Paid at'] = df.groupby('Name')['Paid at'].ffill().bfill()

        df['Shipping Name'] = df.groupby('Name')['Shipping Name'].ffill().bfill()
        df['Shipping Zip'] = df.groupby('Name')['Shipping Zip'].ffill().bfill()
        df['Shipping Country'] = df.groupby('Name')['Shipping Country'].ffill().bfill()

        df['Lineitem price'] = df['Lineitem price'].astype(float).round(2)

    except Exception as e:
        print(f'converting: {e}')

    print('Shopify data transformation complete')

    return df


print(r"""
 ____  _____  _    ____ _____    ____ ___ ____  _____ _     ___ _   _ _____
/ ___||_   _|/ \  |  _ \_   _|  |  _ \_ _|  _ \| ____| |   |_ _| \ | | ____|
\___ \  | | / _ \ | |_) || |    | |_) | || |_) |  _| | |    | ||  \| |  _|
 ___) | | |/ ___ \|  _ < | |    |  __/| ||  __/| |___| |___ | || |\  | |___
|____/  |_/_/   \_\_| \_\|_|    |_|  |___|_|   |_____|_____|___|_| \_|_____|
            """)


def process_file(csv_path: Path, source_name: str) -> pd.DataFrame:
    df = csv_to_dataframe(csv_path)
    if source_name == 'Square':
        df = transform_square(df)
    elif source_name == 'Shopify':
        df = transform_shopify(df)
    else:
        print(f"Unknown source name: {source_name}")
        return pd.DataFrame()
    return df

#----------------------------------------#
# MAIN PIPELINE FUNCTION
#----------------------------------------#

def run_pipeline() -> pd.DataFrame:

    out_columns = [
    'Date',
    'Invoice Id',
    'Item Description', 
    'Customer',
    'Shipping Country',
    'Shipping Postal Code',
    'Number of Units', 
    'Price per Unit',
    ]
    
    # Load configurations
    square_paths_cfg, shopify_paths_cfg = path_config()

    # Process Square CSV file

    square_df = pd.DataFrame()
    if not square_paths_cfg.csv_path.exists():
        print(f"Square CSV file not found: {square_paths_cfg.csv_path}")
    else:
        print(f"[Starting] Processing Square file: {square_paths_cfg.csv_path}")
        square_df = process_file(square_paths_cfg.csv_path, source_name='Square')

    # Process Shopify CSV file

    shopify_df = pd.DataFrame()
    if not shopify_paths_cfg.csv_path.exists():
        print(f"Shopify CSV file not found: {shopify_paths_cfg.csv_path}")
    else:
        print(f"[Starting] Processing Shopify file: {shopify_paths_cfg.csv_path}")
        shopify_df = process_file(shopify_paths_cfg.csv_path, source_name='Shopify')

    print("[Completed] In-memory quarterly royalties ETL")

    square_df.columns = out_columns
    shopify_df.columns = out_columns

    out_df = pd.concat([square_df, shopify_df], ignore_index=True)
    
    out_df = out_df.sort_values(by='Date', ascending=True).reset_index(drop=True)
    
    current_year = datetime.datetime.now().year
    current_quarter = (datetime.datetime.now().month - 1) // 3

    file_name = f"Royalties_report_{current_year}_Q{current_quarter}.csv"
    
    main_output_path = OutputDirConfig().main_output_dir / file_name
    output_path = OutputDirConfig().output_dir / file_name
    
    out_df.to_csv(main_output_path, index=False)
    out_df.to_csv(output_path, index=False)
    return out_df

if __name__ == "__main__":
    out_df = run_pipeline()
