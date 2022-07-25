# TODO: Package all dependencies: cryptography, 
# import pandas as pd
import urllib.parse
import boto3
import logging
from extractcsv import read_csvfile_into_dataframe
from load import *
from transform_3nf import third_normal_form
from  suppress_pii import *
from create_db import *

TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"

s3 = boto3.client('s3')

# s3_r = boto3.resource('s3')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    #Get the object from the event 
    #for i in event['Records']['s3']['bucket']['name']:
    #print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"KEY = {key}")
    file_name = key.split("/")[-1]
    file_path =  '/tmp/' + file_name
    
    is_new_csvfile = is_new_file(file_name)
    LOGGER.info(f'New CSV file: {is_new_csvfile}')
    
    if is_new_csvfile:
        # Download CSV from S3 Bucket
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, file_path)
        # Extract the csv
        data_df = read_csvfile_into_dataframe(file_path)
        
        #Drop the card number column from the dataframe
        drop_column(data_df, "card_number")
        # secretkey = pii.load_key()
        # pii.encrypt_pii(data_df, "customer_name")
        create_hash_feature(data_df,'customer_name')
        # 3. Generate UUID for transactions, then efficently represent the cleaned data
        table_dict = third_normal_form(data_df)     
        
        # SQS
        
        #LOAD
        
        # Connect to Redshift using Psycopg2
        (conn, cursor) = connect_db()
        create_tables(conn, cursor)
        try:
            load_mvp_tables(conn, cursor, table_dict)
            insert_load_date(conn, cursor, file_name, "load_tracker")
        except Exception as e:
            LOGGER.warning(e)
            conn.rollback()
        save_and_close_connection(conn, cursor)
        
        