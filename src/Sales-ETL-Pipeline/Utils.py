import os
import logging
from datetime import datetime
import sys
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from logging_utils.logging_config import setup_logging


#----------------------------------------#
# LOGGING CONFIGURATION
#----------------------------------------#

# help to find logging_utils module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

setup_logging(
    log_file_path=os.getenv('ETL_Sales_to_DB_log_file'),
    project_name= 'SalesETL',
    force_setup=True
)

# initialize logger

logger = logging.getLogger(__name__)

#----------------------------------------#
# ADDITIONAL UTILS FUNCTIONS
#----------------------------------------#

def format_phone_number(phone):
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
    
logger.info('Sales-ETL-Pipeline Utils initialized.')