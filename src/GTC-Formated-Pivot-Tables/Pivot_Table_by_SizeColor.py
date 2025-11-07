
from Utils import log
from CSV_map import size_pattern, color_pattern, greek_name_pattern
from CSV_map import square_split_color_size, shopify_split_color_size
import pandas as pd

#-------------------------------#
# Function to create size and color count pivot table
#-------------------------------#
    
def table_for_size_color_counts(input_df) -> pd.DataFrame:
    
    df_copy = input_df.copy()
    
    if len(df_copy.columns) == 6:
        log('square database detected for size and color count table')
        
        #make map
        df = square_split_color_size(df_copy)

        ptable = df.pivot_table(
            index=['Item Name', 'Size', 'Color'],
            values=['Item Quantity'],
            aggfunc={'Item Quantity': 'sum'}).sort_index()
        
        ptable.loc[('Grand Total', '', ''), 'Item Quantity'] = ptable['Item Quantity'].sum()
        log('grand total added to size and color count table')
        
        return ptable
    
    if len(df_copy.columns) == 4:
        log('shopify database detected for size and color count table')
        #make map
        df = shopify_split_color_size(df_copy)

        ptable = df.pivot_table(
            index=['Lineitem name', 'Size' ,'Greek Name'],
            values=['Lineitem quantity'], 
            aggfunc={'Lineitem quantity': 'sum'}).sort_index()
        
        ptable.loc[('Grand Total', '', ''), 'Lineitem quantity'] = ptable['Lineitem quantity'].sum()
        log('grand total added to size and color count table')
        
        return ptable