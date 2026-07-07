import pathlib
import os
import pandas as pd
import pgeocode

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# get csv files

shopify_df = pd.read_csv(pathlib.Path(r"C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\data\yearly tax reporting\orders_export_1.csv"))
affinity_df = pd.read_csv(pathlib.Path(r"C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\data\yearly tax reporting\gtc_greek_game_time_couture_marketplace_orders_royalty_report-2.csv"))




def get_square_df(input_dir: pathlib.Path, pattern: str = "*.csv") -> pd.DataFrame:
    if not input_dir.exists():
        print(f'Input directory does not exist: {input_dir}')
        return pd.DataFrame()
    csv_files = list(input_dir.glob(pattern))
    if not csv_files:
        print(f'No CSV files found in directory: {input_dir}')
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

square_df = get_square_df(pathlib.Path(r"C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\data\yearly tax reporting\orders-2025-07-02-2026-06-30"))


def add_location_from_zip(df, zip_col):
    nomi = pgeocode.Nominatim('us')

    def lookup_location(zip_code):
        try:
            result = nomi.query_postal_code(str(zip_code).split('-')[0].zfill(5))
            state = result['state_code'] if pd.notna(result['state_code']) else None
            city = result['place_name'] if pd.notna(result['place_name']) else None
            return pd.Series([state, city])
        except Exception:
            return pd.Series([None, None])

    df = df.copy()
    df[['State', 'Shipping City']] = df[zip_col].apply(lookup_location)
    return df




#remove nan values from df
shopify_df = shopify_df[shopify_df['Subtotal'].notna()]
shopify_df = shopify_df[shopify_df['Financial Status'] == 'paid']

affinity_df = affinity_df[affinity_df['Sub-total (before shipping and taxes)'].notna()]

square_df = square_df[square_df['Order Subtotal'].notna()]


# check that data was loaded correctly

shopify_df.info()
# shopify_df['Shipping ']

affinity_df.info()
# affinity_df.head()
square_df.info()
square_df.head(20)




# create subsets of columns

new_shopify_df = shopify_df[['Name','Shipping Name', 'Subtotal', 'Shipping', 'Taxes', 'Total', 'Shipping Province', 'Shipping Country', 'Shipping City', 'Shipping Zip']]

new_affinity_df = affinity_df[['Invoice Id','Customer', 'Sub-total (before shipping and taxes)', 'Customer Total', 'Shipping Fees', 'Shipping Country', 'Shipping Postal Code']]

new_square_df = square_df[['Order', 'Recipient Name', 'Order Subtotal', 'Order Shipping Price', 'Order Tax Total', 'Order Total', 'Recipient Region', 'Recipient Country', 'Recipient City', 'Recipient Postal Code']]
new_square_df = new_square_df.groupby("Order").first().reset_index()
new_square_df.head(20)

# add state from zip
new_affinity_df = add_location_from_zip(new_affinity_df, 'Shipping Postal Code')

new_affinity_df['tax total'] = (new_affinity_df['Customer Total'] - new_affinity_df['Sub-total (before shipping and taxes)'] - new_affinity_df['Shipping Fees']).round(2)

# reorder affinity df columns

new_affinity_df = new_affinity_df[['Invoice Id','Customer', 'Sub-total (before shipping and taxes)', 'Shipping Fees', 'tax total', 'Customer Total', 'State', 'Shipping Country', 'Shipping City', 'Shipping Postal Code']]



new_shopify_df.info()
new_affinity_df.info()
new_square_df.info()


# rename all columns to match output
def rename_columns(df):
    df = df.copy()
    df.columns = ['OrderId','Customer_Name', 'Subtotal', 'Shipping', 'Taxes', 'Total', 'State', 'Country', 'City', 'Zip']
    return df


shopify_out = rename_columns(new_shopify_df)
affinity_out = rename_columns(new_affinity_df)
square_out = rename_columns(new_square_df)

shopify_out.info()
affinity_out.info()
square_out.info()


#append all into on df

all_orders_df = pd.concat([shopify_out, affinity_out, square_out], ignore_index=True)
all_orders_df.info()

#save to csv
output_path = pathlib.Path(r"C:\Users\wiley\Desktop\D3 Design Inc\GTC_code\scripts\scripts output\Yearly Tax Reporting.csv")
all_orders_df.to_csv(output_path, index=False)

print("|"+"-"*100+"|")
print(f"Combined yearly tax reporting data saved to: {output_path}")
print("|"+"-"*100+"|")
