import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()


def log(message: str) -> None:
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(os.getenv('ETL_Sales_to_DB_log_file'), 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')