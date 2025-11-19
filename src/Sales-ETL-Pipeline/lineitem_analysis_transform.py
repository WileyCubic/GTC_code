import pandas as pd
import numpy as np
from Utils import logger
import logging
import re
from load import create_mysql_engine
from extract import query_data
from config import db_config


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


#-------------------------------#
# patterns
#-------------------------------#


size_pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)

color_pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)

greek_name_pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                            Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                            Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta|Penn State|Penn State Theta x Sigma Pi)\b''', re.IGNORECASE)

greek_garment_patterns = re.compile(r'''\b(Love You Cherry Much Hoodie|Signature Stitch Hoodie|Tank|Von Font Hoodie|Rhinestone Kiss Hoodie|Cheetah Applique Hoodie|
                                    Appliqué Wide Leg Sweatpants|Appliqué Mock Neck|Christmas Sisterhood Hoodie|Christmas Sisterhood Flannel PJ Short|Hoodie)\b''', re.IGNORECASE)

square_patterns = re.compile(r'''\b(Cardigan|Crew|Scoop Neck Sweater|Crew Neck Sweater|Tucker Hat|Racing Stripe Sweater|Hats|Twill Visor|Tee|Sweatshirt|Hoodie|Cardigan Sweater|
                             Zip Hoodie|T-Shirt|Sweatshirt|Baseball Cap|Tunic Sweater)\b''', re.IGNORECASE)



df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce').dt.date
df.head(50)








def lineitem_analysis_transform(df: pd.DataFrame) -> pd.DataFrame:

    #format data column 
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce').dt.date
    

    
