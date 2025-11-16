import os
import glob
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

log_file = os.getenv('Delete_log_file')

def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')


ETL_to_table_output_files = glob.glob(os.path.join(os.getenv('ETL_to_table_daily_output'), '*'))
ETL_to_table_output_files
ETL_to_table_input_files = glob.glob(os.path.join(os.getenv('ETL_to_table_daily_csv_input'), '*.csv'))
ETL_to_table_input_files

# Delete output files
for f in ETL_to_table_output_files:
    if os.path.isfile(f):
        os.remove(f)
        log(f'Deleted file: {f}')
        print(f'Deleted file: {f}')
        
# Delete input files
for f in ETL_to_table_input_files:
    if os.path.isfile(f):
        os.remove(f)
        log(f'Deleted file: {f}')
        print(f'Deleted file: {f}')