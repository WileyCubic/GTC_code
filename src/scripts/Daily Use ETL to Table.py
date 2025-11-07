# TO DO:
# --- chart to show what garments are need to be bought form venders --- #
#------------------------------#
# OUTPUT by quantities of item: square ()
# - Table to include:
#   - Garment Name (not directly in data, use the identifyers that are given in the item names)
#   - garment color (not directly in data, will need to be taken form the _______)
#   - garment size (not directly in data, will need to be taken form the ________)
# - Table shouold be ordered by garment name,
# - subtotals for color and size
# - for this only need a count
# - File format:
#   - Excel
# DO NOT include:
# - any information that what was stated above
#------------------------------#
# OUTPUT by quantities of item: shopify ()
# - Table to include:
#   - Garment Name (not directly in data, use the identifyers that are given in the lineitem name)
#   - garment color (not directly in data, will need to be taken form the _______) (still need to be found, might not even be in data at all)
#   - garment size (not directly in data, will need to be taken form the ________) (still need to be found, might not even be in data at all) (garments have different sizes)
# - Table shouold be ordered by garment name,
# - subtotals for color and size
# - for this only need a count
# - File format:
#   - Excel
# DO NOT include:
# - any information that what was stated above
#------------------------------#


# Importing necessary libraries

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import os
from dotenv import load_dotenv
import re
import xlsxwriter as xw
# from sales_CSV_map import create_size_color_maps

# Load environment variables
load_dotenv()

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Optional: Show all columns
pd.set_option('display.width', None)  # Optional: Prevent line wrapping
pd.set_option('display.max_colwidth', None)  # Optional: Show full column content]
pd.set_option('display.float_format', '{:.6f}'.format)  # Optional: Format floats to 2 decimal places 

#------------------------------#
# log processing
#------------------------------#

log_file = os.getenv('ETL_to_table_daily_log_file')


def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

#------------------------------#
# Extracting data from csv files
#------------------------------#

# Path to input folder
csv_input_folder = os.getenv('ETL_to_table_daily_csv_input')
csv_files = glob.glob(csv_input_folder + '/*.csv')
csv_files

# Path to output folder
output_folder = os.getenv('ETL_to_table_daily_output')


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

#------------------------------#
# Function to process input csv files into dataframe
#------------------------------#

# this will only work if all the csv files come from the same source
def csv_to_dataframe(files):
    
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

def clean_input_data(df):
    
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
        'Item Total Price'])
        
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
    'Payment References'])
    
        #formatting columns
        df = df.replace({np.nan: 'None'})
        log('shopify dataframe cleaned')
        return df
    
    else:
        log('unrecognizable database detected')
        return None

#------------------------------#
# Function to create pivot table by item 
#------------------------------#

def create_pivot_table_by_item(df):
    #check which df it is
    #square df
    if len(df.columns) == 6:
        log('square database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Item Name', 'Item Modifiers', 'Item Variation','Order Name'], 
        values=['Item Quantity', 'Item Price'], 
        aggfunc={'Item Quantity': 'sum', 'Item Price': 'first'}).sort_index()
        log('pivot table by item created from square dataframe')
        return ptable
    
    #shopify df
    if len(df.columns) == 4:
        log('shopify database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Lineitem name', 'Shipping Name'], 
        values=['Lineitem quantity', 'Lineitem price'], 
        aggfunc={'Lineitem quantity': 'sum', 'Lineitem price': 'first'}).sort_index()
        log('pivot table by item created from shopify dataframe')
        return ptable

#------------------------------#
# Function to add subtotals and grand totals
#------------------------------#

def add_subtotals_totals_to_by_item(ptable):
    # check which pivot table it is
    #square ptable
    
    # this could potentially be refactored to go baised off of the length of the index
    
    if len(ptable.index.names) == 4:
        log('square pivot table detected\n Adding in subtotals and grand totals')
        total_items_sold = ptable['Item Quantity'].values.sum()
        total_price_sold = (ptable['Item Price'].values * ptable['Item Quantity'].values).sum()
        
        # sub totals of item quantity based on item name
        sub = ptable.groupby(level = 'Item Name')[['Item Quantity']].sum()
        # setting up a multi index for sub totals to be in the correct place on the pivot table
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Item Modifiers': 'SubTotal',
                    'Item Variation': '',
                    'Order Name': ''
                }
            )
        )
        # adding in the sub totals to the end pivot table 
        # they are not in the correct location yet
        out = pd.concat([ptable, sub],axis = 0)
        
        # ordering the pivot table to have subtotals in the correct location
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Item Modifiers'] == 'SubTotal'

        orderer = keys.sort_values(
            ['Item Name', '__is_sub__', 'Item Modifiers', 'Item Variation', 'Order Name'],
        ).index

        out = out.iloc[orderer]
        log('subtotals added')
        
        # adding the grand total to the end of the table
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '', '', '')],
            names=ptable.index.names
        )
        grand_total = pd.DataFrame(
            {"Item Quantity": [total_items_sold],
            "Item Price": [total_price_sold]},
            index=grand_index   
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out
    
    #shopify ptable
    # this needs to be done
    if len(ptable.index.names) == 2:
        log('shopify pivot table detected\n Adding in subtotals and grand totals')
        total_iteams = ptable['Lineitem quantity'].values.sum()
        total_price = (ptable['Lineitem price'].values * ptable['Lineitem quantity'].values).sum()
    
        sub = ptable.groupby(level = 'Lineitem name')[['Lineitem quantity']].sum()
        
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Shipping Name': 'SubTotal'
                }
            )
        )
        
        out = pd.concat([ptable, sub],axis = 0)
        
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Shipping Name'] == 'SubTotal'
        orderer = keys.sort_values(
            ['Lineitem name', '__is_sub__', 'Shipping Name'],
        ).index
        
        out = out.iloc[orderer]
        log('subtotals added')
        
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '')],
            names=ptable.index.names
        )
        
        grand_total = pd.DataFrame(
            {"Lineitem quantity": [total_iteams],
            "Lineitem price": [total_price]},
            index=grand_index
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out

#------------------------------#
# Function to create pivot table by customer name  
# this process need to have log messages looked at
#------------------------------#

def create_pivot_table_by_name(df):
    #check which df it is
    #square df
    if len(df.columns) == 6:
        log('square database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Order Name', 'Item Name', 'Item Modifiers', 'Item Variation'], 
        values=['Item Quantity', 'Item Price'], 
        aggfunc={'Item Quantity': 'sum', 'Item Price': 'first'}).sort_index()
        log('pivot table by customer name created from square dataframe')
        return ptable
    
    #shopify df
    if len(df.columns) == 4:
        log('shopify database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Shipping Name', 'Lineitem name'], 
        values=['Lineitem quantity', 'Lineitem price'], 
        aggfunc={'Lineitem quantity': 'sum', 'Lineitem price': 'first'}).sort_index()
        log('pivot table by customer name created from shopify dataframe')
        return ptable

#------------------------------#
# Function to add subtotals and grand totals
#------------------------------#

def add_subtotals_totals_to_by_name(ptable):
    # check which pivot table it is
    #square ptable
    
    # this could potentially be refactored to go baised off of the length of the index
    
    if len(ptable.index.names) == 4:
        log('square pivot table detected\n Adding in subtotals and grand totals')
        total_items_sold = ptable['Item Quantity'].values.sum()
        total_price_sold = (ptable['Item Price'].values * ptable['Item Quantity'].values).sum()
        
        # sub totals of item quantity based on Order Name
        sub = ptable.groupby(level = 'Order Name')[['Item Quantity']].sum()
        # setting up a multi index for sub totals to be in the correct place on the pivot table
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Item Name': 'SubTotal',
                    'Item Modifiers': '',
                    'Item Variation': ''    
                }
            )
        )
        # adding in the sub totals to the end pivot table 
        # they are not in the correct location yet
        out = pd.concat([ptable, sub],axis = 0)
        
        # ordering the pivot table to have subtotals in the correct location
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Item Name'] == 'SubTotal'

        orderer = keys.sort_values(
            ['Order Name', '__is_sub__', 'Item Name', 'Item Modifiers', 'Item Variation'],
        ).index

        out = out.iloc[orderer]
        log('subtotals added')
        
        # adding the grand total to the end of the table
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '', '', '')],
            names=ptable.index.names
        )
        grand_total = pd.DataFrame(
            {"Item Quantity": [total_items_sold],
            "Item Price": [total_price_sold]},
            index=grand_index   
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out
    
    #shopify ptable
    # this needs to be done
    if len(ptable.index.names) == 2:
        log('shopify pivot table detected\n Adding in subtotals and grand totals')
        total_iteams = ptable['Lineitem quantity'].values.sum()
        total_price = (ptable['Lineitem price'].values * ptable['Lineitem quantity'].values).sum()
    
        sub = ptable.groupby(level = 'Shipping Name')[['Lineitem quantity']].sum()
        
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Lineitem name': 'SubTotal'
                }
            )
        )
        
        out = pd.concat([ptable, sub],axis = 0)
        
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Lineitem name'] == 'SubTotal'
        orderer = keys.sort_values(
            ['Shipping Name', '__is_sub__', 'Lineitem name'],
        ).index
        
        out = out.iloc[orderer]
        log('subtotals added')
        
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '')],
            names=ptable.index.names
        )
        
        grand_total = pd.DataFrame(
            {"Lineitem quantity": [total_iteams],
            "Lineitem price": [total_price]},
            index=grand_index
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out
    
    
#------------------------------#
# Function to add subtotals and grand totals
#this does not work and is still being worked on
#------------------------------#    

#-------------------------------#
# Function to create size and color maps from sales data
#-------------------------------#

size_pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)

color_pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)

greek_name_pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                                Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                                Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta)\b''', re.IGNORECASE)


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


def table_for_size_color_counts(df):
    
    if len(df.columns) == 6:
        log('square database detected for size and color count table')
        #make map
        df = square_split_color_size(df)

        ptable = df.pivot_table(
            index=['Item Name', 'Size', 'Color'],
            values=['Item Quantity'],
            aggfunc={'Item Quantity': 'sum'}).sort_index()
        
        ptable.loc[('Grand Total', '', ''), 'Item Quantity'] = ptable['Item Quantity'].sum()
        log('grand total added to size and color count table')
        
        return ptable
    
        
        
        
    if len(df.columns) == 4:
        log('shopify database detected for size and color count table')
        #make map
        df = shopify_split_color_size(df)

        ptable = df.pivot_table(
            index=['Lineitem name', 'Size' ,'Greek Name'],
            values=['Lineitem quantity'], 
            aggfunc={'Lineitem quantity': 'sum'}).sort_index()
        
        ptable.loc[('Grand Total', '', ''), 'Lineitem quantity'] = ptable['Lineitem quantity'].sum()
        log('grand total added to size and color count table')
        
        return ptable
    

#-------------------------------#
# Export to excel doc
#-------------------------------#
def excel_export(ptable1, ptable2, ptable3):
    #timestamp for file name
    now = datetime.now()
    timestamp = now.strftime('%m-%d-%Y')
    #output path
    output_file_path = os.path.join(output_folder, f'Formated Table {timestamp}.xlsx')
    #exporting to excel
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        ptable1.to_excel(writer, sheet_name='Garment Counts')
        ptable2.to_excel(writer, sheet_name='By Item Name')
        ptable3.to_excel(writer, sheet_name='By Customer Name')
        log(f'Excel file expoort')

#------------------------------#
# process
#------------------------------#
try:
    input = csv_to_dataframe(csv_files)
    log('input dataframe created')
    cleaned = clean_input_data(input)
    log('input dataframe cleaned')
    
    ptable_by_item = create_pivot_table_by_item(cleaned)
    log('pivot table created by item')
    ptable_by_item_with_totals = add_subtotals_totals_to_by_item(ptable_by_item)
    log('subtotals and grand total added to pivot table by item')
    
    patable_by_name = create_pivot_table_by_name(cleaned)
    log('pivot table created by customer name')
    ptable_by_name_with_totals = add_subtotals_totals_to_by_name(patable_by_name)
    log('subtotals and grand total added to pivot table by customer name')
    
    size_and_color = table_for_size_color_counts(cleaned)
    log('size and color table created')
    
    excel_export(size_and_color, ptable_by_item_with_totals, ptable_by_name_with_totals)
    log('excel file exported')
    
except Exception as e:
    log(f'Error: {e}')
