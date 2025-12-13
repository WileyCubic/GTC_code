from Utils import logger
import logging
import pandas as pd
import re
from config import pattern_config

logger = logging.getLogger(__name__)
logger.info("CSV_map module loaded.")


#get patterns from config
patterns = pattern_config()


#-------------------------------#
# Function to create size and color maps from sales data
#-------------------------------#

def square_split_color_size(df):
    
    Itemname = df['Item Name'].astype(str)
    
    variation = df['Item Variation'].astype(str)
    
    # come back and look at this section and make sure the patterns are working correctly

    size = variation.str.extract(patterns.size_pattern, expand=False)
    
    color = variation.str.replace(patterns.size_pattern, '', regex=True)
    
    colorfromname = Itemname.str.extract(patterns.color_pattern, expand=False)
    
    color = (
        color.str.replace(r'\s*,\s*', ', ', regex=True)
        .str.strip(' ,')
    )
    
    color = color.mask(color.eq(''))
    
    df['Color1'] = color
    df['Color from Name'] = colorfromname
    
    df['Color'] = df['Color1'].combine_first(df['Color from Name'])
    df.drop(columns=['Color1', 'Color from Name'], inplace=True)
    
    df['Size'] = size.str.upper()
    
    
    df.drop(columns=['Item Variation'], inplace=True)
    
    logger.info("Square color and size mapping completed.")
    
    return df

def shopify_split_color_size(df):
    
    lineitemname = df['Lineitem name'].astype(str)

    size = lineitemname.str.extract(patterns.size_pattern, expand=False)

    color = lineitemname.str.extract(patterns.color_pattern, expand=False)

    greek_name = lineitemname.str.extract(patterns.organization_pattern, expand=False)

    df['Greek Name'] = greek_name
    
    df['Size'] = size.str.upper()
    
    df['Color'] = color
    
    logger.info("Shopify color and size mapping completed.")
    
    return df