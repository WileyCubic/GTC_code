from dotenv import load_dotenv
load_dotenv()
import pandas as pd

#----------------------------------------#
# maybe more will be added in the future
#----------------------------------------#


#----------------------------------------#
# ADDITIONAL UTILS FUNCTIONS
#----------------------------------------#

def format_phone_number(phone, logger):
    if pd.isna(phone):
        return 0
    phone = str(int(float(phone))) # Convert to string and remove decimal if present
    try:
        if len(phone) == 10:
            return f'({phone[:3]})-{phone[3:6]}-{phone[6:]}'
        elif len(phone) == 11:
            return f'{phone[0]}-({phone[1:4]}) {phone[4:7]}-{phone[7:]}'  
        elif len(phone) == 9:
            return f'({phone[:2]}) {phone[2:5]}-{phone[5:]}'
        elif len(phone) == 12:
            return f'{phone[0:2]}-({phone[2:5]}) {phone[5:8]}-{phone[8:]}'
        else:
            return ValueError
    except Exception as e:
        logger.error(f"Error formatting phone number {phone}: {e}")
        return phone

