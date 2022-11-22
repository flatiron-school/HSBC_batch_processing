'''
Script to reset the data in the three sqlite sales databases.

Use's base_sales.csv and new_sales.csv
'''

import os
import sqlite3
import pandas as pd

# Connections to sqlite databases
sales_con = sqlite3.connect('sales.db')
new_sales_con = sqlite3.connect('new_data.db')

# Import sales and new sales data to pandas DataFrame
sales_df = pd.read_csv('base_sales.csv')
new_sales_df = pd.read_csv('new_sales.csv')


# Replace tables in database with original data
sales_df.to_sql('customer_sales', sales_con, index=False, if_exists='replace')
new_sales_df.to_sql('new_customer_sales', new_sales_con, index=False, if_exists='replace')

# Close connections
sales_con.close()
new_sales_con.close()

# Remove "Data Warehouse" database
os.remove('warehouse.db')