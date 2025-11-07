from Utils import log
from ETL_for_CSV_data import get_input_csv_files, csv_to_dataframe, clean_input_data
# from CSV_map import Size_Color_map
from Pivot_Table_by_SizeColor import table_for_size_color_counts
from Pivot_Table_by_Name import create_pivot_table_by_name, add_subtotals_totals_to_by_name
from Pivot_Table_by_Item import create_pivot_table_by_item, add_subtotals_totals_to_by_item
from Export_to_Excel import get_output_folder, excel_export
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import os
from dotenv import load_dotenv
load_dotenv()
import re
import xlsxwriter as xw

#------------------------------#
# Main function for Formated Pivot Tables
#------------------------------#

def main():

    # Get and clean input data
    
    files = get_input_csv_files()

    input_df = csv_to_dataframe(files)

    cleaned_df = clean_input_data(input_df)
    
    log('###input data cleaned and ready for pivot table creation###')
    
    # Size Color Pivot Table

    size_and_color = table_for_size_color_counts(cleaned_df)  
    log('###size and color pivot table created###')
    

    # By item Pivot Table
    ptable_by_item = create_pivot_table_by_item(cleaned_df)
    log('###by item pivot table created###')
    
    ptable_by_item_with_totals = add_subtotals_totals_to_by_item(ptable_by_item) 
    log('###subtotals and grand totals added to by item pivot table###')
    

    # By Name Pivot Table

    ptable_by_name = create_pivot_table_by_name(cleaned_df)
    log('###by name pivot table created###')

    ptable_by_name_with_totals = add_subtotals_totals_to_by_name(ptable_by_name)
    log('###subtotals and grand totals added to by name pivot table###')

    # Export to Excel

    output_folder = get_output_folder()

    excel_export(size_and_color, ptable_by_item_with_totals, ptable_by_name_with_totals, output_folder)
    log('###all pivot tables exported to excel###')


if __name__ == '__main__':
    main()

