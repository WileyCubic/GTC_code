from Utils import log
from config import DatabaseConfig
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine



def create_mysql_engine(config: DatabaseConfig) -> Engine:
    password = config.password
    url = f'mysql+mysqlconnector://{config.user}:{password}@{config.host}/{config.database}'
    log(f'{config.user} connected to MySQL database')
    return create_engine(url)

    
    


def create_mysql_table_if_replace(df: pd.DataFrame, table_name: str, engine) -> None:
    """Create MySQL table if it does not exist based on DataFrame schema."""
    log(f"Creating table '{table_name}' in MySQL database")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )
    log(f"Table '{table_name}' created successfully")
    
