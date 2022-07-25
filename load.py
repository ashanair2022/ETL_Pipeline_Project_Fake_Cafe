from json import load
import psycopg2
import os
import pandas as pd
import psycopg2.extras as extras
from sqlalchemy import create_engine
from product_query import get_new_products, query_product_ids, replace_index_with_queried_id
from datetime import datetime
from create_db import connect_db, save_and_close_connection, create_load_tracker_table
import time

TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"
#from create_conn_string import *

def load_table(df,table):  #pass in pandasdataframe and table in database
    url = create_url_2()
    db = create_engine(url)
    conn = db.connect()
    df.to_sql(table, con=conn, if_exists='append', index=False)

    conn.close()
    

def load_table_2(conn, cursor, df:pd.DataFrame, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Convert DataFrame into a list of tuples
    tuples_list = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ', '.join(list(df.columns))

    insert_query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    # insert_query  = "INSERT INTO \"dev_delon6_team1\".\"public\".\"%s\"(%s) VALUES %%s" % (table, cols)
    try:
        extras.execute_values(cursor, insert_query, tuples_list)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()


def insert_load_date(conn, cursor, file_name, table):
    load_date = str(datetime.today().strftime('%Y-%m-%d'))
    # load_tuple = (file_name, load_date)
    cols = "csv_filename, load_date"
    insert_query  = "INSERT INTO %s(%s) VALUES ('%s', '%s')" % (table, cols, file_name, load_date)
    cursor.execute(insert_query)
    conn.commit()
    

def is_new_file(file_name):
    # Connect to 
    (conn, cursor) = connect_db()
    # Create load_tracker if not exists
    create_load_tracker_table(conn, cursor)
    load_data_query = f'SELECT csv_filename FROM "dev_delon6_team1"."public"."load_tracker" WHERE csv_filename = \'{file_name}\''
    cursor.execute(load_data_query)
    load_tracker_data=cursor.fetchone()
    save_and_close_connection(conn, cursor)
    return not(bool(load_tracker_data))


def load_mvp_tables(conn, cursor, table_dict):
    """
    Load the three tables into database: transactions, products and then basket_items 
    Each item of table_dict has the form (table_name, table_df)
    """
    start_load = datetime.now().microsecond
    transactions_df = table_dict[TRANSACTIONS_TABLE]
    # load_table_2(conn, cursor, transactions_df, TRANSACTIONS_TABLE)
    products_df = table_dict[PRODUCTS_TABLE]
    baskets_df = table_dict[BASKET_ITEMS_TABLE]
    print("Determining new products")
    new_products_df = get_new_products(conn, cursor, products_df)                      #TODO: Call the function defined in product_query.py
    print(new_products_df)
    time.sleep(30)
    print("Loading new products")
    load_table_2(conn, cursor, new_products_df, PRODUCTS_TABLE)
    time.sleep(30)
    load_table_2(conn, cursor, transactions_df, TRANSACTIONS_TABLE)
    mid_load = datetime.now().microsecond
    print(f"1st Half Load Time = {mid_load - start_load}")
    query_product_ids(conn, cursor, products_df)
    replace_index_with_queried_id(baskets_df, products_df)
    load_table_2(conn, cursor, baskets_df, BASKET_ITEMS_TABLE)
    end_load = datetime.now().microsecond
    print(f"2nd Half Load Time = {end_load - mid_load}")
