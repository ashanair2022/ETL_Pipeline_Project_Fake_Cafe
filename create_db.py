# Thought: How often should we connect and then close the connection?
# from datetime import datetime
import json
import psycopg2 # To connect to PostgreSQL Server
import boto3

def load_db_credentials():
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name='redshift-cluster-master-pass', WithDecryption=True)
    password = parameter['Parameter']['Value']
    
    parameter = ssm.get_parameter(Name='team1-redshift-credentials', WithDecryption=True)
    db_dict = json.loads(parameter['Parameter']['Value'])
    db_dict["password"] = password
    return db_dict


#connect to RSDB
def connect_db():
    db_dict = load_db_credentials()

    conn = psycopg2.connect(dbname = db_dict['dbname'],
                            host = db_dict["endpoint"],
                            port = db_dict['port'],
                            user = db_dict['login'],
                            password = db_dict["password"])

    # conn = psycopg2.connect(dbname = os.environ.get(dbname),
    #                         host = os.environ.get(host),
    #                         port = os.environ.get(port),
    #                         user = os.environ.get(user),
    #                         password = password)
   
    return (conn, conn.cursor())




# Thought: Could error handle connection, if we fail to connect
# def connect_to_db():
#     load_dotenv()
#     db_host = os.environ.get("postgresql_host")
#     db_user = os.environ.get("postgresql_user")
#     db_password = os.environ.get("postgresql_pass")
#     db = os.environ.get("postgresql_db")

#     conn = psycopg2.connect(
#         host = db_host,
#         user = db_user,
#         password = db_password,
#         dbname = db
#     )
#     return (conn, conn.cursor())

# Save changes to DB and then close cursor + connection
def save_and_close_connection(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()

# Create tables if they don't exist: products, customers, stores, transactions
def create_tables(conn, cursor):
    # CREATE SEQUENCE IF NOT EXISTS products_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
    # "id" integer DEFAULT nextval('products_id_seq') NOT NULL,
    #  WITH (oids = false)
    
    create_products_table = \
        """
        CREATE TABLE IF NOT EXISTS "public"."products" (
            "id" BIGINT IDENTITY(1,1),
            "name" VARCHAR(100),
            "size" VARCHAR(20),
            "flavour" VARCHAR(100),
            CONSTRAINT "products_pkey" PRIMARY KEY ("id")
        );
        """ 
    # Create the transactions table
    # CREATE SEQUENCE IF NOT EXISTS "transactions_ID_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
    # "id" integer DEFAULT nextval('"transactions_ID_seq"') NOT NULL,
    create_transaction_table = \
        """
        CREATE TABLE IF NOT EXISTS "public"."transactions" (
            "id" BIGINT IDENTITY(1,1),
            "transaction_id" VARCHAR(36) NOT NULL,
            "timestamp" timestamp NOT NULL,
            "store" VARCHAR(100),
            "customer_name" VARCHAR(300),
            "total_price" numeric(6,2),
            "cash_or_card" VARCHAR(10),
            CONSTRAINT "transactions_pkey" PRIMARY KEY ("id"),
            CONSTRAINT "transactions_transaction_id" UNIQUE ("transaction_id")
        );
        """
    #  CREATE SEQUENCE IF NOT EXISTS "basket_items_ID_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
    # "id" integer DEFAULT nextval('"basket_items_ID_seq"') NOT NULL,
    create_basket_items_table=\
        """
        CREATE TABLE IF NOT EXISTS "public"."basket_items" (
            "id" BIGINT IDENTITY(1,1),
            "transaction_id" VARCHAR(36) NOT NULL REFERENCES transactions(transaction_id),
            "product_id" BIGINT NOT NULL REFERENCES products(id),
            "price" numeric(5,2),
            "quantity" INT,
            CONSTRAINT "basket_items_pkey" PRIMARY KEY ("id")
        );
        """

        # ALTER TABLE ONLY "public"."basket_items" ADD CONSTRAINT "basket_items_product_id_fkey" FOREIGN KEY (product_id) REFERENCES products(id) NOT DEFERRABLE;
        # ALTER TABLE ONLY "public"."basket_items" ADD CONSTRAINT "basket_items_transaction_id_fkey" FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) NOT DEFERRABLE;

    # create_etl_last_run_table=\
    #     """
    #     DROP TABLE IF EXISTS "etl_last_run";
    #     CREATE TABLE "public"."etl_last_run" (
    #         "last_run_time" timestamp NOT NULL
    #     ) WITH (oids = false);
    #     """

        # CREATE TABLE IF NOT EXISTS transactions(
        #     id SERIAL PRIMARY KEY,
        #     timestamp TIMESTAMP,
        #     store_id int NOT NULL REFERENCES stores (id),
        #     customer_id int NOT NULL REFERENCES customers (id),
        #     product_id int NOT NULL REFERENCES products (id),
        #     quantity int,
        #     cash_card VARCHAR(10)
        # );
        # """       

    cursor.execute(create_products_table)
    cursor.execute(create_transaction_table)
    cursor.execute(create_basket_items_table)
    # cursor.execute(create_etl_last_run_table)
    conn.commit()


def create_load_tracker_table(conn, cursor):
    create_load_tracker_table=\
        """
        CREATE TABLE IF NOT EXISTS "public"."load_tracker" (
            "id" BIGINT IDENTITY(1,1),
            "csv_filename" VARCHAR(100) NOT NULL,
            "load_date" DATE,
            CONSTRAINT "load_tracker_pkey" PRIMARY KEY ("id")
        );
        """
    cursor.execute(create_load_tracker_table)
    conn.commit()


def create_mvp_tables(conn,cursor):
    create_products_trfmd_table = \
        """
        CREATE TABLE IF NOT EXISTS products(
            id SERIAL PRIMARY KEY,
            name VARCHAR(200),
            size VARCHAR(10),
            flavour VARCHAR(200)
        );
        """
    
    #Almost fully Transformed tables but need to reference productsmvp (id)
    create_transactions_trfmd_table = \
        """
        CREATE TABLE IF NOT EXISTS transactions(
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            timestamp TIMESTAMP,
            store VARCHAR(200),
            customer_name VARCHAR(300),
            total_price  DECIMAL(19,2),
            cash_or_card VARCHAR(10)
        );
        """ 
    create_basket_items_trmd_table = \
        """
        CREATE TABLE IF NOT EXISTS basket_items(
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(36) NOT NULL,
            product_id int NOT NULL,
            price DECIMAL(19, 2) NOT NULL,
            quantity int NOT NULL
        );
        """       
    cursor.execute(create_products_trfmd_table)
    cursor.execute(create_transactions_trfmd_table)
    cursor.execute(create_basket_items_trmd_table)
    conn.commit()


# # Prevents calling those methods when importing to another module
# if __name__ ==  "__main__":
#     (conn, cursor) = connect_to_db()
    # create_tables(conn, cursor)
    # create_mvp_tables(conn,cursor)
    # save_and_close_connection(conn, cursor)

