import pandas as pd
import numpy as np
from Utils import logger
import logging
import re
from load import create_mysql_engine
from extract import query_data
from config import db_config, lineitem_patterns_config


db = db_config()
engine = create_mysql_engine(db)

query = '''
select `Order Date` as OrderDate,
       `Item Quantity` as ItemQuantity,
       CONCAT_WS(' - ', `Item Name`, `Item Variation`) as ItemName,
       `Item Price` as ItemPrice,
       'Square' as Source
from square_raw

UNION ALL

select `Paid at` as OrderDate,
       `Lineitem quantity` as ItemQuantity,
       `Lineitem name` as ItemName,
       `Lineitem price` as ItemPrice,
       'Shopify' as Source
from shopify_raw

ORDER BY OrderDate DESC;
'''

df = query_data(engine, query)
df.shape
df.head(50)


logger = logging.getLogger(__name__)
logger.info("Starting Lineitem Analysis Transformation")


df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce').dt.date
df.head(50)


# patterns
patterns = lineitem_patterns_config()
patterns.size_pattern
patterns.color_pattern
patterns.greek_name_pattern
patterns.greek_garment_patterns
patterns.square_patterns

def lineitem_analysis_transform(df: pd.DataFrame) -> pd.DataFrame:

    #format data column 
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce').dt.date
    

    
