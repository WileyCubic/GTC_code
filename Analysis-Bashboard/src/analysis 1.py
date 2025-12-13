import os
import sqlite3
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
load_dotenv()
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
# pd.set_option('display.float_format', '{:.6f}'.format)

log_file = os.getenv('Query_log_file')
#writing both to daily log and global log
def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    log("MySQL environment variables loaded successfully")
except Exception as e:
    log(f"ERROR: loading MySQL environment variables: {e}")

try:
    Mysql_connection = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    if Mysql_connection.is_connected():
        log("Successfully connected to MySQL database")
except mysql.Error as e:
    log(f"ERROR: connecting to MySQL Platform: {e}")


# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
log("MySQL cursor created")

# MySQL SQLAlchemy engine
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
log("MySQL SQLAlchemy engine created successfully")

# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    log(f'Executed: {query}')
    return df


bp_expences_query = '''
select sum(Amount) 
from bp_expences;
'''
ab_expences_query = '''
select sum(Amount)
from ab_expences;
'''
squre1 = '''
select `Item Quantity`, `Item Price`, `Order Date`, `Item Name`
from square_orders;
'''
shopify1 = '''
select `Lineitem quantity`, `Lineitem price`, `Paid at`
from shopify_orders;
'''


bp_expences = sql_query(bp_expences_query, mysql_engine)
ab_expences = sql_query(ab_expences_query, mysql_engine)
square1 = sql_query(squre1, mysql_engine)
shopify1 = sql_query(shopify1, mysql_engine)

bp_expences
ab_expences
total_expences = bp_expences + ab_expences
total_expences

square1['Temp total'] = square1['Item Quantity'] * square1['Item Price']
square1_total = square1['Temp total'].sum()
square1.info()
square1['Rollling Total'] = square1['Temp total'].cumsum()
square1['Order Date'] = pd.to_datetime(square1['Order Date'])
square1.set_index('Order Date', inplace=True)
square1.head(20)

shopify1['Temp total'] = shopify1['Lineitem quantity'] * shopify1['Lineitem price']
shopify1_total = shopify1['Temp total'].sum()
shopify1_total
shopify1['Rolling Total'] = shopify1['Temp total'].cumsum()
shopify1['Paid at'] = pd.to_datetime(shopify1['Paid at'])
shopify1.set_index('Paid at', inplace=True)

total_sales = square1_total + shopify1_total
total_sales

bp_time_query = '''
select Date, Amount
from bp_expences;
'''
bp_expences_time = sql_query(bp_time_query, mysql_engine)
bp_expences_time['Date'] = pd.to_datetime(bp_expences_time['Date'])
bp_expences_time['Rolling Total'] = bp_expences_time['Amount'].cumsum()
bp_expences_time.set_index('Date', inplace=True)
bp_expences_time.info()
bp_expences_time.head(20)

ab_time_query = '''
select Date, Amount
from ab_expences;
'''
ab_expences_time = sql_query(ab_time_query, mysql_engine)
ab_expences_time['Date'] = pd.to_datetime(ab_expences_time['Date'])
ab_expences_time['Rolling Total'] = ab_expences_time['Amount'].cumsum()
ab_expences_time.set_index('Date', inplace=True)

plt.figure(figsize=(14, 12))
sns.lineplot(data=square1, x=square1.index, y='Rollling Total', color='Green')
sns.lineplot(data=shopify1, x=shopify1.index, y='Rolling Total', color='Blue')
sns.lineplot(data=bp_expences_time, x=bp_expences_time.index, y='Rolling Total', color='Red')
sns.lineplot(data=ab_expences_time, x=ab_expences_time.index, y='Rolling Total', color='Orange')
plt.title('Money In and Out Over Time') 
plt.xlabel('Date')
plt.ylabel('Money Amount')
plt.grid(True)
plt.tight_layout()
plt.legend(labels=['Square Sales', 'Shopify Sales', 'BP Expences'])
plt.show()

fig = go.Figure()
fig.add_trace(go.Scatter(x=square1.index, y=square1['Rollling Total'], mode='lines', name='Square Sales', line=dict(color='Green')))
fig.add_trace(go.Scatter(x=shopify1.index, y=shopify1['Rolling Total'], mode='lines', name='Shopify Sales', line=dict(color='Blue')))
fig.update_layout(title='Money In Over Time', xaxis_title='Date', yaxis_title='Money Amount')
fig.show()

fig = go.Figure()
fig.add_trace(go.Scatter(x=bp_expences_time.index, y=bp_expences_time['Rolling Total'], mode='lines', name='BP Expences', line=dict(color='Red')))
fig.add_trace(go.Scatter(x=ab_expences_time.index, y=ab_expences_time['Rolling Total'], mode='lines', name='AB Expences', line=dict(color='Orange')))
fig.update_layout(title='Money Out Over Time', xaxis_title='Date', yaxis_title='Money Amount')
fig.show()


square2_grouped = square1.groupby('Item Name').agg({'Item Quantity': 'sum', 'Temp total': 'sum'}).reset_index().sort_values(by = 'Temp total', ascending=False)
square2_grouped





# Close MySQL connection

try:
    Mysql_cursor.close()
    Mysql_connection.close()
    log("MySQL connection closed")
except Exception as e:
    log(f"ERROR: closing MySQL connection: {e}")
    
    
