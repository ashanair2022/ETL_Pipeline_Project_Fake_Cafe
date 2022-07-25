import pandas as pd
import numpy as np
import csv
# from create_db import connect_t_db
import uuid
#from product_query import query_id
TEST_CSV = "test.csv" # Located in 'src' folder
BASKET_COLUMN = "basket_items"
# Columns to be extracted for products_df, index column acts as a local primary key
PRODUCT_COLUMNS = ["name", "size", "flavour"]
TRANSACTION_UUID = "transaction_id"
# Columns to be extracted for transactions_df, after UUIDs have been generated
TRANSACTION_COLUMNS = [TRANSACTION_UUID, "timestamp", "store", "customer_name", "total_price", "cash_or_card"]
# TRANSACTION_COLUMNS = ["transaction_id","timestamp", "store", "customer_name", "total_price", "cash_or_card"]
TRANSACTION_ID = "transaction_id"
PRODUCT_ID = "product_id"
BASKET_ITEMS_COLUMNS = [TRANSACTION_ID, PRODUCT_ID, "price", "quantity"]
# BASKET_ITEMS_COLUMNS = ["transaction_id", "product_id", "price", "quantity","timestamp", "store", "customer_name", "total_price", "cash_or_card","basket_items"]
TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"

# Transform data_frame to 1st Normal Form
# We will split the basket items into separate rows after each comma (counting quantity per basket)
def split_basket_items(df:pd.DataFrame) -> pd.DataFrame:
    # df[TRANSACTION_ID] = df.apply(lambda _: uuid.uuid4(), axis=1)
    #df[TRANSACTION_ID] = df.index
    def count_items(basket: str):
        item_count_dict = {}
        items_list = [item.strip() for item in basket.split(",")]
        for item in items_list:
            item_count_dict[item] = item_count_dict.get(item, 0) + 1
        return list(item_count_dict.items())
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(lambda x: count_items(x))
    return df.explode(BASKET_COLUMN, ignore_index=True)

# Split the product column into separate columns for each detail: size, name, flavour, price, quantity
def extract_product_details(df:pd.DataFrame) -> pd.DataFrame:
    # Extract the details of product: size, name, flavour, price
    def extract_details(product_count:tuple) -> list:
        product = product_count[0]
        size_plus_other_details = product.strip().split(" ", 1)
        size = size_plus_other_details[0]
        other_details = [detail.strip() for detail in size_plus_other_details[1].split("-")]
        if len(other_details) == 2:
            price = other_details[-1]
            other_details[-1] = "Original"
            other_details.append(price)
        name = other_details[0]
        if name[:9] == "Flavoured":
            other_details[0] = name[9:].lstrip().capitalize()
        details = [size]
        details.extend(other_details)
        details.append(product_count[1])
        return details
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(extract_details)
    df[['size','name','flavour','price','quantity']] = pd.DataFrame(df[BASKET_COLUMN].to_list())
    del df[BASKET_COLUMN]
    return df

# Extract some columns for a subtable: 'transactions' and 'products', and replace with row_index
def extract_subtable(df:pd.DataFrame, subcolumns:list, foreign_key:str):
    subtable_df = df[subcolumns].drop_duplicates()

    if foreign_key != TRANSACTION_ID:
        subtable_df['combined'] = subtable_df.astype(str).apply(np.sum, axis=1)
        
        def get_index(combined_value:str, subtable_df:pd.DataFrame):
            try:
                return subtable_df.index[subtable_df['combined'] == combined_value].tolist()[0]
            except:
                return None

        df[foreign_key] = df[subcolumns].astype(str).apply(np.sum, axis=1).apply(lambda x: get_index(x, subtable_df))
        del subtable_df['combined']
    else:
        df[TRANSACTION_ID] = df[TRANSACTION_UUID] # UUIDs need to be generated before extraction

    for column in subcolumns:
        if column != TRANSACTION_ID:
            del df[column]
    
    return subtable_df


# Normalise the transactions: Replace product details with product_id

# Transform data_frame to 3rd Normal Form

def third_normal_form(data_df: pd.DataFrame) -> dict:
    #TODO: Add UUIDs in column data_df[TRANSACTION_UUID]
    data_df[TRANSACTION_UUID] = data_df.apply(lambda _: uuid.uuid4(), axis=1).astype(str)
    # Start 3NF
    transactions_df = extract_subtable(data_df, TRANSACTION_COLUMNS, TRANSACTION_ID)
    # customers_df = extract_subtable(transactions_df, ['customer_name'], 'customer_id')
    # stores_df = extract_subtable(transactions_df, ['store'], 'store_id')
    basket_df = split_basket_items(data_df)
    extract_product_details(basket_df)
    products_df = extract_subtable(basket_df, PRODUCT_COLUMNS, PRODUCT_ID)
    
    return {
        PRODUCTS_TABLE: products_df,
        TRANSACTIONS_TABLE: transactions_df,
        BASKET_ITEMS_TABLE: basket_df[BASKET_ITEMS_COLUMNS]
    }

# After querying ids, replace them in the basket_table
def replace_index_with_queried_id(df):

    df['product_id'] = df['basket_items'].apply(query_id) # Get the actual product ids against each basket item 
    return df

# Gets the products ids for a basket its from the database

def query_id(basket_item):
    (conn, cursor) = connect_to_db()
    
    product_name = basket_item[1]
    product_size = basket_item[0]
    product_flavour = basket_item[2]
    values = [product_name, product_size, product_flavour]
    cursor.execute(f'SELECT id FROM products WHERE name = %s AND size = %s AND flavour = %s', values)
    sql_id = cursor.fetchone()[0]

    return sql_id

if __name__ == "__main__":
    
    
    data_df = pd.read_csv(TEST_CSV)
    del data_df['card_number']
    #print(data_df)
    rt=third_normal_form(data_df)
    
    rt['basket_items']=replace_index_with_queried_id(rt['basket_items'])
    rt['basket_items']=rt['basket_items'].drop(['timestamp',
       'store', 'customer_name', 'total_price', 'cash_or_card',
       'basket_items'],axis=1)
    
    print('PRODUCTS DATAFRAME')
    print(rt['products'])
    print('TRANSACTIONS DATAFRAME')
    print(rt['transactions'].head())
    print('BASKET DATAFRAME')
    print(rt['basket_items'].head(20))
   

