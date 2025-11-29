import pandas as pd
import numpy as np
from Utils import logger
import logging
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime

from config import lineitem_patterns_config


# Create logger for file

logger = logging.getLogger(__name__)
logger.info("Starting Lineitem Analysis Transformation")

#----------------------------------------#
# DATA TRANSFORMATION FOR LINEITEM ANALYSIS
#----------------------------------------#

def transform_lineitem_analysis(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming data for Lineitem Analysis")
    
    # Normalize OrderDate to datetime while keeping failures as NaT for investigation.

    df['OrderDate'] = df['OrderDate'].astype(str).str.slice(0, 10).apply(pd.to_datetime, format='%Y-%m-%d', errors='coerce')
    logger.info("OrderDate column transformed")
    logger.info("Converted OrderDate column to datetime with %s null values", df['OrderDate'].isna().sum())


    df.fillna(
        {
            'ShippingLocation': 'InPerson',
        },
        inplace=True
    )
    logger.info("Filled missing ShippingLocation values with 'InPerson'")
    
    
    # Compile regex patterns used to extract attributes from ItemName.
    patterns = lineitem_patterns_config()
    logger.info("Loaded regex patterns from config")

    # Extract attributes using regex patterns.
    
    size = df['ItemName'].str.extract(patterns.size_pattern, expand=False)
    color = df['ItemName'].str.extract(patterns.color_pattern, expand=False)
    organization = df['ItemName'].str.extract(patterns.organization_pattern, expand=False)
    garment_type = df['ItemName'].str.extract(patterns.garment_patterns, expand=False)
    
    logger.info("Extracted attributes using regex patterns")
    
    # Create new columns for Size, Color, Organization Name, and Garment Type.

    df['Size'] = size.str.upper() 
    df['Color'] = color
    df['Organization Name'] = organization
    df['Garment Type'] = garment_type

    logger.info(
    "Extracted attributes and Created new columns - missing counts Size:%s Color:%s Org:%s Garment:%s",
    df['Size'].isna().sum(),
    df['Color'].isna().sum(),
    df['Organization Name'].isna().sum(),
    df['Garment Type'].isna().sum()
    )


    # Compute simple monetary metrics for exploratory plots.

    df['money_value'] = df['ItemQuantity'] * df['ItemPrice']
    df['Rolling Total'] = df['money_value'].cumsum()
    
    logger.info("Added columns for monetary calculations")

    return df








