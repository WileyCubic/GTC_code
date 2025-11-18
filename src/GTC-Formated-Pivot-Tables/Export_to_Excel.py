from Utils import logger
import logging
import os
from datetime import datetime
import pandas as pd
import xlsxwriter
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
logger.info("Export_to_Excel module loaded.")

#------------------------------#
# Get the output folder path
#------------------------------#

def get_output_folder() -> str:
    output_folder = os.getenv('ETL_to_table_daily_output')
    logger.debug(f'[Export_to_Excel]: output folder is {output_folder}')
    return output_folder

#------------------------------#
# Export pivot tables to single excel workbook
#------------------------------#


def excel_export(ptable1, ptable2, ptable3, output_folder) -> None:
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
        logger.info(f'Excel file exported to {output_file_path}')