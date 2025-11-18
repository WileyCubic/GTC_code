# not working at the moment as mysql package not updated


from Utils import logger
import logging
import os
import sqlite3
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
load_dotenv()
from datetime import datetime

logger = logging.getLogger(__name__)
logger.info("ETL_from_query module loaded.")

# MySQL Database Connection

def load_mysql_env_vars():
    try:
        host= os.getenv('Mysql_host')
        user= os.getenv('Mysql_user')
        password= os.getenv('Mysql_password')
        database= os.getenv('Mysql_database')
        
        logger.info("MySQL environment variables loaded successfully")
        
    except Exception as e:
        
        logger.error(f"loading MySQL environment variables: {e}")
    return host, user, password, database
        
def mysql_connect(host, user, password, database):
    try:
        Mysql_connection = mysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if Mysql_connection.is_connected():
            logger.info("Successfully connected to MySQL database")
    except mysql.Error as e:
        logger.error(f"connecting to MySQL Platform: {e}")
        
    return Mysql_connection


# Create a cursor object and MySQL query function

#MySQL database cursor
def mysql_cursor(Mysql_connection):
    
    Mysql_cursor = Mysql_connection.cursor()
    
    logger.info("MySQL cursor created")
    return Mysql_cursor

# MySQL SQLAlchemy engine
def mysql_sqlalchemy_engine(user, password, host, database):
    
    mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    
    logger.info("MySQL SQLAlchemy engine created successfully")
    
    return mysql_engine

# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    logger.info(f'Executed: {query}')
    return df



# this need to go into main script later as it will have input that will need to be called
# Query need to get data
squre_query = '''

'''



shopify_query = '''

'''