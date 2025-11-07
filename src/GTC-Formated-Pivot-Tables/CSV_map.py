from Utils import log
import pandas as pd
import re


#-------------------------------#
# patterns
#-------------------------------#


size_pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)

color_pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)

greek_name_pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                            Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                            Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta)\b''', re.IGNORECASE)

#-------------------------------#
# Function to create size and color maps from sales data
#-------------------------------#

def square_split_color_size(df):
    
    Itemname = df['Item Name'].astype(str)
    
    variation = df['Item Variation'].astype(str)

    size = variation.str.extract(size_pattern, expand=False)
    
    color = variation.str.replace(size_pattern, '', regex=True)
    
    colorfromname = Itemname.str.extract(color_pattern, expand=False)
    
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
    
    
    return df

def shopify_split_color_size(df):
    
    lineitemname = df['Lineitem name'].astype(str)

    size = lineitemname.str.extract(size_pattern, expand=False)
    
    color = lineitemname.str.extract(color_pattern, expand=False)
    
    greek_name = lineitemname.str.extract(greek_name_pattern, expand=False)
    
    df['Greek Name'] = greek_name
    
    df['Size'] = size.str.upper()
    
    df['Color'] = color
    
    
    return df