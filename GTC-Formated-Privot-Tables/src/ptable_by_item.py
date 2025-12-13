
import logging
from Utils import logger
import pandas as pd

logger = logging.getLogger(__name__)
logger.info("Pivot_Table_by_Item module loaded.")

#------------------------------#
# Function to create pivot table by customer name  
# this process need to have log messages looked at
#------------------------------#

def create_pivot_table_by_item(df):
    #check which df it is
    #square df
    if len(df.columns) == 6:

        logger.debug('square database detected for pivot table creation')
        
        ptable = df.pivot_table(
            index=['Item Name', 'Item Modifiers', 'Item Variation','Order Name'], 
            values=['Item Quantity', 'Item Price'], 
            aggfunc={'Item Quantity': 'sum', 'Item Price': 'first'}
        ).sort_index()
            
        logger.info('pivot table by item created from square dataframe')

        return ptable
    
    #shopify df
    if len(df.columns) == 4:
        
        logger.debug('shopify database detected for pivot table creation')
        
        ptable = df.pivot_table(
            index=['Lineitem name', 'Shipping Name'], 
            values=['Lineitem quantity', 'Lineitem price'], 
            aggfunc={'Lineitem quantity': 'sum', 'Lineitem price': 'first'}
        ).sort_index()
        
        logger.info('pivot table by item created from shopify dataframe')
        
        return ptable


