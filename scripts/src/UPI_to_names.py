
import pandas as pd

from royalty_scripts.config import royaltyReportPathsConfig
#make sure to load environment variables

config = royaltyReportPathsConfig()


#pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


#+--------------------------------------------------+
# Script to map UPI codes to product names using a CSV file #
#+--------------------------------------------------+
# get all files and combine into one dataframe
Q1 = pd.read_csv(config.Q1_path)
Q2 = pd.read_csv(config.Q2_path)
Q3 = pd.read_csv(config.Q3_path)
Q4 = pd.read_csv(config.Q4_path)

all_data = pd.concat([Q1, Q2, Q3, Q4], ignore_index=True)

all_data.info()


needed_df = all_data[['Affinity UPI', 'Item Description', 'Affinity Product Category']]

needed_df.describe()
needed_df['Item Description2'] = needed_df['Item Description']

upi_ptable = needed_df.pivot_table(
    index='Affinity UPI',
    values=['Item Description', 'Affinity Product Category', 'Item Description2'],
    aggfunc={'Item Description': pd.Series.unique, 'Affinity Product Category': 'first', 'Item Description2':pd.Series.nunique}
    ).sort_index()
upi_ptable.reset_index(inplace=True)
upi_ptable.rename(columns={'Item Description': 'Product Names', 'Affinity Product Category': 'Product Categories', 'Item Description2': 'Unique Product Names Count'}, inplace=True)

out_df = pd.DataFrame(upi_ptable)

OUT_DIR = config.output_dir
out_path = OUT_DIR / 'UPI_to_names_output.csv'

out_df.to_csv(out_path, index=False)

