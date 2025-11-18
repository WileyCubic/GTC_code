import pandas as pd
import numpy as np
from Utils import logger
import logging
import re



logger = logging.getLogger(__name__)
logger.info("Starting Lineitem Analysis Transformation")


#-------------------------------#
# patterns
#-------------------------------#


size_pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)

color_pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)

greek_name_pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                            Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                            Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta)\b''', re.IGNORECASE)

