from Utils import logger
import logging

from config import path_config

from ETL_from_csv import get_input_csv_files, csv_to_dataframe, clean_input_data
# from ETL_from_query import query_df, clean_query_data

from ptable_for_garment import table_for_size_color_counts
from ptable_by_item import create_pivot_table_by_item
from ptable_by_customer import create_pivot_table_by_name
from ptable_grand_sub_totals import add_totals_to_by_item, add_totals_to_by_name

from export_to_excel import excel_export


logger = logging.getLogger(__name__)
logger.info("[Formate-pipe]: main pipeline started")

def FPT_pipe():
    
    # load configs
    output_config, input_config = path_config()
    input_dir = input_config.Input_dir
    output_dir = output_config.Output_dir
    logger.info("[Formate-pipe]: configurations loaded")
    
    # etl from csv

    input_files = get_input_csv_files(input_dir)
    input_df = csv_to_dataframe(input_files)
    cleaned_df = clean_input_data(input_df)
    logger.info(f"[Formate-pipe]: {len(cleaned_df)} rows loaded from csv files and cleaned")

    
    # size and color pivot table
    try:
        size_and_color_ptable = table_for_size_color_counts(cleaned_df)
        logger.info("[Formate-pipe]: size and color pivot table created")
    except Exception as e:
        logger.error(f"[Formate-pipe]: size and color pivot table creation failed | {e}")
        return e

    # by item pivot table
    try:
        item = create_pivot_table_by_item(cleaned_df)
        ptable_by_item = add_totals_to_by_item(item)
        logger.info("[Formate-pipe]: by item pivot table created")
    except Exception as e:
        logger.error(f"[Formate-pipe]: by item pivot table creation failed | {e}")
        return e
    
    # by name pivot table
    try:
        name = create_pivot_table_by_name(cleaned_df)
        ptable_by_name = add_totals_to_by_name(name)
        logger.info("[Formate-pipe]: by name pivot table created")
    except Exception as e:
        logger.error(f"[Formate-pipe]: by name pivot table creation failed | {e}")
        return e

    logger.info("[Formate-pipe]: by name pivot table created")
    
    # export to excel
    
    excel_export(size_and_color_ptable, ptable_by_item, ptable_by_name, output_dir)
    
    logger.info("[Formate-pipe]: all pivot tables exported to excel")
    
if __name__ == "__main__":
    FPT_pipe()