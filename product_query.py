# Situation product details from the dataframe has been loaded into the table

# Import libraries
import pandas as pd
PRODUCTS_COLUMNS = ['name', 'size', 'flavour']
# Query database with unique combinations of name, size, flavour and price
# Connection and cursor objects already exist
# Access the values in DF given the row index
def query_id(conn, cursor, row_index: int, products_df:pd.DataFrame):
    
    products_name = products_df['name'].loc[row_index]
    products_size = products_df['size'].loc[row_index]
    products_flavour = products_df['flavour'].loc[row_index]
    values = [products_name, products_size, products_flavour]
    cursor.execute(f'SELECT id FROM "dev_delon6_team1"."public"."products" WHERE name = \'{products_name}\' AND size = \'{products_size}\' AND flavour = \'{products_flavour}\'')
    sql_id = cursor.fetchone()[0]

    return sql_id

# Store queried products id's in a separate column
def query_product_ids(conn, cursor, df:pd.DataFrame):
    df['queried_id'] = df.index
    df['queried_id'] = df['queried_id'].apply(lambda x : query_id(conn, cursor, x, df))

# After querying ids, replace them in the basket_table
def replace_index_with_queried_id(df, products_df):
    df['product_id'] = df['product_id'].apply(lambda x: products_df['queried_id'].loc[x])
    return df

# TODO: Define function that determines which products are new. Then call it in load.py
def get_new_products(conn, cursor, df):
        products_query='SELECT * FROM "dev_delon6_team1"."public"."products"'
        cursor.execute(products_query)
        pdt_table_data=cursor.fetchall()
        pdt_table_data_df=pd.DataFrame.from_records(pdt_table_data, columns=[x[0] for x in cursor.description])
        pdt_table_data_df['master'] = 'master'
        pdt_table_data_df.set_index('master', append=True, inplace=True)
        #print(pdt_table_data_df)
        df['daily'] = 'daily'
        df.set_index('daily', append=True, inplace=True)
        #print(df)

        merged = pdt_table_data_df.append(df)
        #print(merged)
        merged = merged.drop_duplicates(subset=['name', 'size','flavour'], keep=False).sort_index()
        #print('Merged:',merged)
        idx = pd.IndexSlice
        #print(idx)
        if not merged.empty:
                merged=merged.loc[idx[:, 'daily'], :]
                #print(merged)
                merged=merged.reset_index(drop=True)
                #print(merged)
        return merged.drop(['id'],axis=1)
        #print(df)
        #return df
        # 

def get_new_products_2(df, pdt_table_data_df):
    pdt_table_data_df['is_new'] = 'master'
    # pdt_table_data_df.set_index('master', append=True, inplace=True)
    #print(pdt_table_data_df)
    df['is_new'] = 'daily'
    # df.set_index('daily', append=True, inplace=True)
    #print(df)

    merged = pdt_table_data_df.append(df)
    #print(merged)
    merged = merged.drop_duplicates(subset=['name', 'size','flavour'], keep=False).sort_index()
    new_products = merged[merged["is_new"] == "daily"]
    #print('Merged:',merged)
    # idx = pd.IndexSlice
    #print(idx)
    # if not merged.empty:
    #         merged=merged.loc[idx[:, 'daily'], :]
    #         print("\nIt has merged\n")
    #         merged=merged.reset_index(drop=True)
            #print(merged)
    # return merged.drop(['id'],axis=1)
    return new_products[PRODUCTS_COLUMNS]



                                                    
if __name__ == "__main__":
    pdt_table_data_df = pd.read_csv("products.csv")
    products_df = pd.read_csv("new_products.csv")
    print(get_new_products_2(products_df, pdt_table_data_df))

    # from transform_3nf import * # Only needed when testing out how they combine
    # from create_db import *
    # Connect to db and store in variable by creating the connection object and the cursor object
    # (conn, cursor) = connect_to_db()
    # df = pd.read_csv(TEST_CSV)
    # del df['card_number']
    # df = split_basket_items(df)
    # extract_product_details(df)
    # products_df = create_products_df(df)
    # transactions_df = create_transactions_df(df, products_df)
    # query_product_ids(conn, cursor, products_df)
    # print(products_df)
    # print()
    # print(transactions_df)
    # print()
    # replace_index_with_queried_id(transactions_df, products_df)
    # print(transactions_df)


    
    