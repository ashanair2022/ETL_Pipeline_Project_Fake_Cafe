import codecs
import datetime
import pandas as pd
import csv
import urllib.parse
import boto3
import logging
import sqlalchemy
import os
from extractcsv import read_csvfile_into_dataframe
#from test_table import create_sales_tables
from load import  load_table
#from transform_3nf import *
#from suppress_pii import encrypt_pii,decrypt_pii
s3 = boto3.client('s3')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    #Get the object from the event 
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"KEY = {key}")

    # Use boto3 to download the event s3 object key to the /tmp directory.
    file_name =  '/tmp/' + key.split("/")[-1]
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, file_name)
    lst = os.listdir()
    print(lst)

    # Use pandas to read the csv.
     #Extract the csv
    df = read_csvfile_into_dataframe(file_name)
   

    # Log the dataframe head.
    print(df)