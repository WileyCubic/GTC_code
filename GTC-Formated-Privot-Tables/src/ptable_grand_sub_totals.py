
import pandas as pd
from Utils import logger
import logging
logger = logging.getLogger(__name__)





#------------------------------#
# Function to add subtotals and grand totals
#------------------------------#

def add_totals_to_by_item(ptable) -> pd.DataFrame:
    # check which pivot table it is
    #square ptable
    
    # this could potentially be refactored to go baised off of the length of the index
    
    if len(ptable.index.names) == 4:
        logger.debug('square pivot table detected')
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
        logger.info('subtotals added')
        
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
        logger.info('grand total added')
        return out
    
    #shopify ptable
    # this needs to be done
    if len(ptable.index.names) == 2:
        logger.debug('shopify pivot table detected\n Adding in subtotals and grand totals')
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
        logger.info('subtotals added')
        
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
        logger.info('grand total added')
        return out


#------------------------------#
# Function to add subtotals and grand totals
#------------------------------#

def add_totals_to_by_name(ptable):
    # check which pivot table it is
    #square ptable
    
    # this could potentially be refactored to go baised off of the length of the index
    
    if len(ptable.index.names) == 4:
        logger.debug('square pivot table detected | Adding in subtotals and grand totals')
        total_items_sold = ptable['Item Quantity'].values.sum()
        total_price_sold = (ptable['Item Price'].values * ptable['Item Quantity'].values).sum()
        
        # sub totals of item quantity based on Order Name
        sub = ptable.groupby(level = 'Order Name')[['Item Quantity']].sum()
        # setting up a multi index for sub totals to be in the correct place on the pivot table
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Item Name': 'SubTotal',
                    'Item Modifiers': '',
                    'Item Variation': ''    
                }
            )
        )
        # adding in the sub totals to the end pivot table 
        # they are not in the correct location yet
        out = pd.concat([ptable, sub],axis = 0)
        
        # ordering the pivot table to have subtotals in the correct location
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Item Name'] == 'SubTotal'

        orderer = keys.sort_values(
            ['Order Name', '__is_sub__', 'Item Name', 'Item Modifiers', 'Item Variation'],
        ).index

        out = out.iloc[orderer]
        logger.info('subtotals added')
        
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
        logger.info('grand total added')
        return out
    
    #shopify ptable
    # this needs to be done
    if len(ptable.index.names) == 2:
        logger.debug('shopify pivot table detected Adding in subtotals and grand totals')
        total_iteams = ptable['Lineitem quantity'].values.sum()
        total_price = (ptable['Lineitem price'].values * ptable['Lineitem quantity'].values).sum()
    
        sub = ptable.groupby(level = 'Shipping Name')[['Lineitem quantity']].sum()
        
        sub.index = pd.MultiIndex.from_frame(
            sub.index.to_frame().assign(
                **{
                    'Lineitem name': 'SubTotal'
                }
            )
        )
        
        out = pd.concat([ptable, sub],axis = 0)
        
        keys = out.index.to_frame(index=False)
        keys['__is_sub__'] = keys['Lineitem name'] == 'SubTotal'
        orderer = keys.sort_values(
            ['Shipping Name', '__is_sub__', 'Lineitem name'],
        ).index
        
        out = out.iloc[orderer]
        logger.info('subtotals added')
        
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
        logger.info('grand total added')
        return out