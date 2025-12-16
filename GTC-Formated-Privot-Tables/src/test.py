from ETL_from_csv import get_input_csv_files, csv_to_dataframe, clean_input_data
from csv_map import square_split_color_size


from config import path_config


from logging_config import logger
import logging
logger.info("[Test]: test script started")


# load configs
output_config, input_config = path_config()
input_dir = input_config.Input_dir
output_dir = output_config.Output_dir
logger.info("[Test]: configurations loaded")

# etl from csv

input_files = get_input_csv_files(input_dir)
input_df = csv_to_dataframe(input_files)
cleaned_df = clean_input_data(input_df)
logger.info(f"[Test]: {len(cleaned_df)} rows loaded from csv files and cleaned")


cleaned_df.info()

# size and color pivot table
df = square_split_color_size(cleaned_df)
df.fillna('',inplace=True)
df.info()

ptable = df.pivot_table(
    index=['Item Name', 'Size', 'Color'],
    values=['Item Quantity'],
    aggfunc={'Item Quantity': 'sum'}
).sort_index()

ptable.loc[('Grand Total', '', ''), 'Item Quantity'] = ptable['Item Quantity'].sum()

ptable