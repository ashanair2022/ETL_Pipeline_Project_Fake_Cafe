# This file should be run only on the day when the data is finally required to be loaded to Redshift and only once.
#Therefore it is a one-time function
# For testing, a dummy database could be created to load data. 

import codecs
import datetime
import pandas as pd
import csv
import urllib.parse

import boto3


s3 = boto3.client('s3')
s3_r = boto3.resource('s3')


def lambda_handler(event, context):
    print('Hello')

    # Get the object from the event 
    bucket = event['Records'][0]['s3']['bucket']['name']
    my_bucket=s3_r.Bucket(bucket)#key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    #To read the files loaded to S3 on days before the day when the data is loaded to Redshift. 
    loading_day=datetime.datetime.now().strftime("%d")
    
    start_day=10                # The files started to be uploaded on 10 June 2022
    previous_days=[]            #  An empty list to append the previous days from the date of loading data to database
    
    for day in range(start_day,int(loading_day)):
        previous_days.append(day)
    print(previous_days)
    
    dir_path="2022/6/"
    filepaths=[]
    for dt in range(len(previous_days)):
        filepath=dir_path+str(previous_days[dt])
        filepaths.append(filepath)
    print(filepaths)

    filenames=[]
    for data_file in filepaths:
        for object_summary in my_bucket.objects.filter(Prefix=data_file):
                filenames.append(object_summary.key)
    print(filenames)        

    for file in filenames:
        try:
            response = s3.get_object(Bucket=bucket, Key=file)
            
            #rows=['Date,Store,Name,Product,PaymentType,Price,Transaction_Id']
            rows=[]
            for row in csv.DictReader(codecs.getreader('utf-8')(response['Body'])):
    	        rows.append(row)
    	   
            df = pd.DataFrame(rows)
            print(df.head())
            
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
