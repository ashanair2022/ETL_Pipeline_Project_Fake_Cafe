from extractcsv import read_csvfile_into_dataframe
import suppress_pii as pii
from transform_3nf import third_normal_form
from create_db import connect_to_db, create_tables, save_and_close_connection
from load import load_mvp_tables

FILE_NAME = "test.csv" # stored in src/ directory

def etl_file(file_name: str):
    # EXTRACT
    # 1. Gain access to CSV landed in AWS S3 bucket (eg download it)
    # 2. Read CSV into a Pandas DataFrame
    data_df = read_csvfile_into_dataframe(file_name)
    
    # TRANSFORM
    # 1. Format timestamp (Done in 'read_csvfile_into_dataframe') and drop duplicates
    # data_df.drop_duplicates()
    # 2. Suppress PII with secret key stored in AWS SSM
    pii.drop_column(data_df, "card_number")
    pii.encrypt_pii(data_df, "customer_name")
    # 3. Generate UUID for transactions, then efficently represent the cleaned data
    table_dict = third_normal_form(data_df)     # TODO: In line 92, add UUID
    
    # SQS
     
    # LOAD
    # 1. Connect to database (To connect to AWS Redshift, use the one defined in the AWS console)
    (conn, cursor) = connect_to_db()
    # 2. Create tables if they don't exist: transactions, products, basket_items
    create_tables(conn, cursor)
    #(conn, cursor) # TODO: Adjust so that it works for Redshift
    # 3. Load transformed tables
    load_mvp_tables(conn, cursor, table_dict)   # TODO: Determine new products
    # 4. Save changes and close connection
    save_and_close_connection(conn, cursor)

if __name__ == "__main__":
    # Ensure .env file has 'postgresql_db' and 'conn_string' for 'test' database.
    # Ensure Docker containers for PostgreSQL database are running.
    # Ensure to pip install all dependencies
    # When first running locally, ensure those tables don't already exist in 'test' database.
    etl_file(FILE_NAME)
