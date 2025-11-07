import os
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
load_dotenv()
from datetime import datetime
import re

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

log_file = os.getenv('Sales_CSV_Map_log_file')


def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    log("MySQL environment variables loaded successfully")
except Exception as e:
    log(f"ERROR: loading MySQL environment variables: {e}")

try:
    Mysql_connection = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    if Mysql_connection.is_connected():
        log("Successfully connected to MySQL database")
except mysql.Error as e:
    log(f"ERROR: connecting to MySQL Platform: {e}")


# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
log("MySQL cursor created")

# MySQL SQLAlchemy engine
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
log("MySQL SQLAlchemy engine created successfully")

# Pull data from MYSQL to df for mapping

map_query_square = '''
select DISTINCT `Item Name`, `Item Variation`
from square_orders;
'''

map_query_shopify = '''
select distinct `Lineitem name`
from shopify_orders;
'''


def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    log(f'Executed: {query}')
    return df


# transform Item Varition into color and size columns

size_pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)

color_pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)

greek_name_pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                                Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                                Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta)\b''', re.IGNORECASE)


#used for square data and is working as intended

def square_split_color_size(df):
    
    Itemname = df['Item Name'].astype(str)
    
    variation = df['Item Variation'].astype(str)

    size = variation.str.extract(size_pattern, expand=False)
    
    color = variation.str.replace(size_pattern, '', regex=True)
    
    colorfromname = Itemname.str.extract(color_pattern, expand=False)
    
    color = (
        color.str.replace(r'\s*,\s*', ', ', regex=True)
        .str.strip(' ,')
    )
    
    color = color.mask(color.eq(''))
    
    df['Color1'] = color
    df['Color from Name'] = colorfromname
    
    df['Color'] = df['Color1'].combine_first(df['Color from Name'])
    df.drop(columns=['Color1', 'Color from Name'], inplace=True)
    
    df['Size'] = size.str.upper()
    
    
    df.drop(columns=['Item Variation'], inplace=True)
    
    
    return df

def shopify_split_color_size(df):
    
    lineitemname = df['Lineitem name'].astype(str)

    size = lineitemname.str.extract(size_pattern, expand=False)
    
    color = lineitemname.str.extract(color_pattern, expand=False)
    
    greek_name = lineitemname.str.extract(greek_name_pattern, expand=False)
    
    df['Greek Name'] = greek_name
    
    df['Size'] = size.str.upper()
    
    df['Color'] = color
    
    
    return df


def export_to_csv(df, filename):
    #                                  Output Path                  Name of output file
    output_name = os.path.join(os.getenv('CSV_map_output'), f'{filename}.csv')
    try:
        df.to_csv(output_name, index=False)
        log(f"{filename} data exported to CSV")
    except Exception as e:
        log(f"ERROR: exporting data to CSV: {e}")

#test

# Main process

def create_size_color_maps_exports():
    square_map_query = sql_query(map_query_square, mysql_engine)
    log('Square map df created')
    square_map = square_split_color_size(square_map_query)
    log('Square map df done')
    export_to_csv(square_map, 'square_map')
    log('Square map csv exported')

    shopify_map_query = sql_query(map_query_shopify, mysql_engine)
    log('Shopify map df created')
    shopify_map = shopify_split_color_size(shopify_map_query)
    log('Shopify map df done')
    export_to_csv(shopify_map, 'shopify_map')
    log('Shopify map csv exported')

def create_size_color_maps():
    square_map_query = sql_query(map_query_square, mysql_engine)
    log('Square map df created')
    square_map = square_split_color_size(square_map_query)
    log('Square map df done')


    shopify_map_query = sql_query(map_query_shopify, mysql_engine)
    log('Shopify map df created')
    shopify_map = shopify_split_color_size(shopify_map_query)
    log('Shopify map df done')

    return square_map, shopify_map










