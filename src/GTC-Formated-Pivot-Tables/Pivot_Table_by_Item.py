
from Utils import log
import pandas as pd



#------------------------------#
# Function to create pivot table by customer name  
# this process need to have log messages looked at
#------------------------------#

def create_pivot_table_by_item(df):
    #check which df it is
    #square df
    if len(df.columns) == 6:
        log('square database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Item Name', 'Item Modifiers', 'Item Variation','Order Name'], 
        values=['Item Quantity', 'Item Price'], 
        aggfunc={'Item Quantity': 'sum', 'Item Price': 'first'}).sort_index()
        log('pivot table by item created from square dataframe')
        return ptable
    
    #shopify df
    if len(df.columns) == 4:
        log('shopify database detected for pivot table creation')
        ptable = df.pivot_table(
        index=['Lineitem name', 'Shipping Name'], 
        values=['Lineitem quantity', 'Lineitem price'], 
        aggfunc={'Lineitem quantity': 'sum', 'Lineitem price': 'first'}).sort_index()
        log('pivot table by item created from shopify dataframe')
        return ptable

#------------------------------#
# Function to add subtotals and grand totals
#------------------------------#

def add_subtotals_totals_to_by_item(ptable) -> pd.DataFrame:
    # check which pivot table it is
    #square ptable
    
    # this could potentially be refactored to go baised off of the length of the index
    
    if len(ptable.index.names) == 4:
        log('square pivot table detected\n Adding in subtotals and grand totals')
        total_items_sold = ptable['Item Quantity'].values.sum()
        total_price_sold = (ptable['Item Price'].values * ptable['Item Quantity'].values).sum()
        
        # sub totals of item quantity based on item name
        sub = ptable.groupby(level = 'Item Name')[['Item Quantity']].sum()
        # setting up a multi index for sub totals to be in the correct place on the pivot table
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Item Modifiers': 'SubTotal',
                    'Item Variation': '',
                    'Order Name': ''
                }
            )
        )
        # adding in the sub totals to the end pivot table 
        # they are not in the correct location yet
        out = pd.concat([ptable, sub],axis = 0)
        
        # ordering the pivot table to have subtotals in the correct location
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Item Modifiers'] == 'SubTotal'

        orderer = keys.sort_values(
            ['Item Name', '__is_sub__', 'Item Modifiers', 'Item Variation', 'Order Name'],
        ).index

        out = out.iloc[orderer]
        log('subtotals added')
        
        # adding the grand total to the end of the table
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '', '', '')],
            names=ptable.index.names
        )
        grand_total = pd.DataFrame(
            {"Item Quantity": [total_items_sold],
            "Item Price": [total_price_sold]},
            index=grand_index   
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out
    
    #shopify ptable
    # this needs to be done
    if len(ptable.index.names) == 2:
        log('shopify pivot table detected\n Adding in subtotals and grand totals')
        total_iteams = ptable['Lineitem quantity'].values.sum()
        total_price = (ptable['Lineitem price'].values * ptable['Lineitem quantity'].values).sum()
    
        sub = ptable.groupby(level = 'Lineitem name')[['Lineitem quantity']].sum()
        
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Shipping Name': 'SubTotal'
                }
            )
        )
        
        out = pd.concat([ptable, sub],axis = 0)
        
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Shipping Name'] == 'SubTotal'
        orderer = keys.sort_values(
            ['Lineitem name', '__is_sub__', 'Shipping Name'],
        ).index
        
        out = out.iloc[orderer]
        log('subtotals added')
        
        grand_index = pd.MultiIndex.from_tuples(
            [('Grand Total', '')],
            names=ptable.index.names
        )
        
        grand_total = pd.DataFrame(
            {"Lineitem quantity": [total_iteams],
            "Lineitem price": [total_price]},
            index=grand_index
        )
        
        out = pd.concat([out, grand_total], axis=0)
        log('grand total added')
        return out
