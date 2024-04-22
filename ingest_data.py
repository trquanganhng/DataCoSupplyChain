import os
import argparse

import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    url = params.url

    if url.endswith('.csv.zip'):
        csv_name = 'output.csv.zip'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name, encoding='ISO-8859-1')

    create_tables_queries = [
        """
        DROP TABLE IF EXISTS "Shipping" CASCADE;
        CREATE TABLE "Shipping" (
            "row_id" SERIAL PRIMARY KEY,
            "ship_id" int    NOT NULL,
            "order_id" int    NOT NULL,
            "ship_date" timestamp    NOT NULL,
            "days_real" int    NOT NULL,
            "days_scheduled" int    NOT NULL,
            "ship_status" int    NOT NULL,
            "ship_mode" int    NOT NULL
        )
        """,
        """
        DROP TABLE IF EXISTS "ShippingMode" CASCADE;
        CREATE TABLE "ShippingMode" (
            "shipMode_id" int   NOT NULL,
            "shipMode_name" text    NOT NULL,
            CONSTRAINT "pk_ShippingMode" PRIMARY KEY ("shipMode_id"),
            CONSTRAINT "uc_ShippingMode_shipMode_name" UNIQUE ("shipMode_name")
        )
        """,
        """
        DROP TABLE IF EXISTS "ShippingStatus" CASCADE;
        CREATE TABLE "ShippingStatus" (
            "status_id" int   NOT NULL,
            "status_name" text   NOT NULL,
            CONSTRAINT "pk_ShippingStatus" PRIMARY KEY ("status_id"),
            CONSTRAINT "uc_ShippingStatus_status_name" UNIQUE ("status_name")
        )
        """,
        """
        DROP TABLE IF EXISTS "Customer" CASCADE;
        CREATE TABLE "Customer" (
            "cust_id" int   NOT NULL,
            "cust_Fname" text   NOT NULL,
            "cust_Lname" text   NOT NULL,
            "cust_email" text   NOT NULL,
            "cust_segment" text   NOT NULL,
            "custAdd_id" int   NOT NULL,
            CONSTRAINT "pk_Customer" PRIMARY KEY ("cust_id")
        )
        """,
        """
        DROP TABLE IF EXISTS "CustomerAddress" CASCADE;
        CREATE TABLE "CustomerAddress" (
            "custAdd_id" SERIAL PRIMARY KEY,
            "custAdd_street" text   NOT NULL,
            "custAdd_city" varchar(200)   NOT NULL,
            "custAdd_zipcode" int   NOT NULL,
            "custAdd_state" varchar(200)   NOT NULL,
            "custAdd_country" varchar(200)   NOT NULL
        )
        """,
        """
        DROP TABLE IF EXISTS "Order" CASCADE;
        CREATE TABLE "Order" (
            "row_id" SERIAL PRIMARY KEY,
            "order_id" int   NOT NULL,
            "order_date" timestamp   NOT NULL,
            "prod_id" int   NOT NULL,
            "quantity" int   NOT NULL,
            "cust_id" int   NOT NULL,
            "department_id" int   NOT NULL,
            "TotalAmount" money   NOT NULL,
            "order_status" int   NOT NULL,
            "orderAdd_id" int   NOT NULL
        )
        """,
        """
        DROP TABLE IF EXISTS "OrderAddress" CASCADE;
        CREATE TABLE "OrderAddress" (
            "orderAdd_id" SERIAL PRIMARY KEY,
            "orderAdd_city" varchar(200)   NOT NULL,
            "orderAdd_state" varchar(200)   NOT NULL,
            "orderAdd_country" varchar(200)   NOT NULL,
            "orderAdd_region" varchar(200)   NOT NULL,
            "orderAdd_market" int   NOT NULL
        )
        """,
        """
        DROP TABLE IF EXISTS "OrderStatus" CASCADE;
        CREATE TABLE "OrderStatus" (
            "status_id" int   NOT NULL,
            "status_name" text   NOT NULL,
            CONSTRAINT "pk_OrderStatus" PRIMARY KEY ("status_id"),
            CONSTRAINT "uc_OrderStatus_status_name" UNIQUE ("status_name")
        )
        """,
        """
        DROP TABLE IF EXISTS "Department" CASCADE;
        CREATE TABLE "Department" (
            "department_id" int   NOT NULL,
            "department_name" text   NOT NULL,
            CONSTRAINT "pk_Department" PRIMARY KEY ("department_id")
        )
        """,
        """
        DROP TABLE IF EXISTS "Product" CASCADE;
        CREATE TABLE "Product" (
            "prod_id" int   NOT NULL,
            "prodCat_id" int   NOT NULL,
            "prod_name" varchar(200)   NOT NULL,
            "prod_price" money   NOT NULL,
            "prod_status" int   NOT NULL,
            "prod_describe" text   NOT NULL,
            "prod_image" text   NOT NULL,
            CONSTRAINT "pk_Product" PRIMARY KEY ("prod_id"),
            CONSTRAINT "uc_Product_prod_name" UNIQUE ("prod_name")
        )
        """,
        """
        DROP TABLE IF EXISTS "Category" CASCADE;
        CREATE TABLE "Category" (
            "cat_id" int   NOT NULL,
            "cat_name" text   NOT NULL,
            CONSTRAINT "pk_Category" PRIMARY KEY ("cat_id")
        )
        """,
        """
        DROP TABLE IF EXISTS "Market" CASCADE;
        CREATE TABLE "Market" (
            "market_id" int   NOT NULL,
            "market_name" text   NOT NULL,
            CONSTRAINT "pk_Market" PRIMARY KEY ("market_id"),
            CONSTRAINT "uc_Market_market_name" UNIQUE ("market_name")
        )
        """
    ]

    create_relationships_queries = [
        """
        ALTER TABLE "Shipping" ADD CONSTRAINT "fk_Shipping_order_id" FOREIGN KEY("order_id") 
        REFERENCES "Order" ("row_id")
        """,
        """
        ALTER TABLE "Shipping" ADD CONSTRAINT "fk_Shipping_ship_status" FOREIGN KEY("ship_status") 
        REFERENCES "ShippingStatus" ("status_id")
        """,
        """
        ALTER TABLE "Shipping" ADD CONSTRAINT "fk_Shipping_ship_mode" FOREIGN KEY("ship_mode")
        REFERENCES "ShippingMode" ("shipMode_id")
        """,
        """
        ALTER TABLE "Customer" ADD CONSTRAINT "fk_Customer_custAdd_id" FOREIGN KEY("custAdd_id")
        REFERENCES "CustomerAddress" ("custAdd_id")
        """,
        """
        ALTER TABLE "Order" ADD CONSTRAINT "fk_Order_item_id" FOREIGN KEY("prod_id")
        REFERENCES "Product" ("prod_id")
        """,
        """
        ALTER TABLE "Order" ADD CONSTRAINT "fk_Order_cust_id" FOREIGN KEY("cust_id")
        REFERENCES "Customer" ("cust_id")
        """,
        """
        ALTER TABLE "Order" ADD CONSTRAINT "fk_Order_department_id" FOREIGN KEY("department_id")
        REFERENCES "Department" ("department_id")
        """,
        """
        ALTER TABLE "Order" ADD CONSTRAINT "fk_Order_order_status" FOREIGN KEY("order_status")
        REFERENCES "OrderStatus" ("status_id")
        """,
        """
        ALTER TABLE "Order" ADD CONSTRAINT "fk_Order_orderAdd_id" FOREIGN KEY("orderAdd_id")
        REFERENCES "OrderAddress" ("orderAdd_id")
        """,
        """
        ALTER TABLE "OrderAddress" ADD CONSTRAINT "fk_OrderAddress_orderAdd_market" FOREIGN KEY("orderAdd_market")
        REFERENCES "Market" ("market_id")
        """,
        """
        ALTER TABLE "Product" ADD CONSTRAINT "fk_Product_prodCat_id" FOREIGN KEY("prodCat_id")
        REFERENCES "Category" ("cat_id")
        """,
        """
        CREATE INDEX "idx_Customer_cust_Lname"
        ON "Customer" ("cust_Lname")
        """
    ]

    def execute_queries(queries):
        conn = None
        try:
            conn = psycopg2.connect(
                dbname= db,
                user= user,
                password= password,
                host= host,
                port= port
            )
            cur = conn.cursor()

            for query in queries:
                cur.execute(query)

            conn.commit()
            print("Queries executed successfully!")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_data_to_tables(df):
    
        ## Insert data to tables Market, OrderStatus, ShippingMode, ShippingStatus
        def insert_data_to_tables_1(df):
            try:
                # Connection to Database
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )
            
                market_names = df['Market'].unique()
                order_statuses = df['Order Status'].unique()
                shipping_modes = df['Shipping Mode'].unique()
                delivery_statuses = df['Delivery Status'].unique()

                # Enumerate the unique values to assign unique identifiers
                market_id_name_mapping = {index + 1: status for index, status in enumerate(market_names)}
                ostatus_id_name_mapping = {index + 1: status for index, status in enumerate(order_statuses)}
                shipMode_id_name_mapping = {index + 1: shipMode for index, shipMode in enumerate(shipping_modes)}
                dstatus_id_name_mapping = {index + 1: status for index, status in enumerate(delivery_statuses)}

                cur = conn.cursor()
                # Insert datas into tables
                for market_id, market_name in market_id_name_mapping.items():
                    insert_query = """
                    INSERT INTO "Market" ("market_id", "market_name")
                    VALUES (%s, %s)
                    """
                    cur.execute(insert_query, (market_id, market_name))
                    
                for status_id, status_name in ostatus_id_name_mapping.items():
                    insert_query = """
                    INSERT INTO "OrderStatus" ("status_id", "status_name")
                    VALUES (%s, %s)
                    """
                    cur.execute(insert_query, (status_id, status_name))
                    
                for shipMode_id, shipMode_name in shipMode_id_name_mapping.items():
                    insert_query = """
                    INSERT INTO "ShippingMode" ("shipMode_id", "shipMode_name")
                    VALUES (%s, %s)
                    """
                    cur.execute(insert_query, (shipMode_id, shipMode_name))
                    
                for status_id, status_name in dstatus_id_name_mapping.items():
                    insert_query = """
                    INSERT INTO "ShippingStatus" ("status_id", "status_name")
                    VALUES (%s, %s)
                    """
                    cur.execute(insert_query, (status_id, status_name))

                conn.commit()
                cur.close()
                conn.close()
                print("Data inserted into tables Market, OrderStatus, ShippingMode, ShippingStatus successfully!")
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()
        
        ## Insert data to tables Category, Department
        def insert_data_to_tables_2(df):
            try:
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )

                categories = df[["Category Id", "Category Name"]].sort_values(by="Category Id")

                departments = df[["Department Id", "Department Name"]].sort_values(by="Department Id")

                cur = conn.cursor()

                for index, row in categories.iterrows():
                    insert_query = """
                    INSERT INTO "Category" ("cat_id", "cat_name")
                    VALUES (%s, %s)
                    ON CONFLICT ("cat_id") DO NOTHING;
                    """
                    cur.execute(insert_query, (row["Category Id"], row["Category Name"]))
                    
                for index, row in departments.iterrows():
                    insert_query = """
                    INSERT INTO "Department" ("department_id", "department_name")
                    VALUES (%s, %s)
                    ON CONFLICT ("department_id") DO NOTHING;
                    """
                    cur.execute(insert_query, (row["Department Id"], row["Department Name"]))

                conn.commit()
                cur.close()
                conn.close()
                print("Data inserted into tables Category, Department successfully!")
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()
        
        ## Insert data to table Product       
        def insert_data_to_tables_3(df):
            try:
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )
                
                products = df[["Product Card Id", "Product Category Id", "Product Description", "Product Image",
                            "Product Name", "Product Price", "Product Status"]]
                products = products.sort_values(by="Product Card Id")

                customer = df[["Customer Id", "Customer Fname", "Customer Lname", "Customer Segment",
                            "Customer Email"]]

                cur = conn.cursor()

                for index, row in products.iterrows():
                    insert_query = """
                    INSERT INTO "Product" ("prod_id", "prodCat_id", "prod_describe", 
                    "prod_image", "prod_name", "prod_price", "prod_status")
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ("prod_name") DO NOTHING
                    """
                    cur.execute(insert_query, (row["Product Card Id"], row["Product Category Id"], 
                                                row["Product Description"], row["Product Image"],
                                                row["Product Name"], row["Product Price"], row["Product Status"])
                            )

                conn.commit()
                cur.close()
                conn.close()
                print("Data inserted into table Product successfully!")
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()
        
        ## Insert data to tables Customer, CustomerAddress       
        def insert_data_to_tables_4(df):
            try:
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )
                ## Customer, CustomerAddress
                customer = df[["Customer Id", "Customer Fname", "Customer Lname", "Customer Segment",
                            "Customer Email", "Customer City", "Customer Country", "Customer State",
                            "Customer Street", "Customer Zipcode"]].drop_duplicates(subset=["Customer Id"])

                customer["Customer Zipcode"] = customer["Customer Zipcode"].fillna(-1).astype(int)

                cur = conn.cursor()

                customer_data_insert_query = """
                INSERT INTO "Customer" ("cust_id", "cust_Fname", "cust_Lname", "cust_email", "cust_segment", "custAdd_id")
                VALUES (%s, %s, %s, %s, %s, %s);
                """

                customer_address_data_insert_query = """
                INSERT INTO "CustomerAddress" ("custAdd_id", "custAdd_street", "custAdd_city", "custAdd_zipcode", "custAdd_state", "custAdd_country")
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                custAdd_id = 1

                for index, row in customer.iterrows():
                    cur.execute(customer_address_data_insert_query, (custAdd_id, row["Customer Street"], row["Customer City"], row["Customer Zipcode"], row["Customer State"], row["Customer Country"]))
                    cur.execute(customer_data_insert_query, (row["Customer Id"], row["Customer Fname"], row["Customer Lname"], row["Customer Email"], row["Customer Segment"], custAdd_id))
                    custAdd_id += 1
                    
                conn.commit()
                cur.close()
                conn.close()
                print("Data inserted into tables Customer, CustomerAddress successfully!")
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()
        
        ## Insert data to tables Order, OrderAddress       
        def insert_data_to_tables_5(df):
            try:
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )
                # ignore SettingWithCopyWarning warning
                pd.set_option('chained_assignment',None)

                order = df[["Order Id", "order date (DateOrders)", "Order Item Cardprod Id", "Order Item Quantity", "Customer Id", "Department Id", "Order Item Total", "Order Status",
                            "Order City", "Order State", "Order Country", "Order Region", "Market"]]
                # mapping data to get market_id
                query = 'SELECT "market_id", "market_name" FROM "Market";'
                market_mapping = pd.read_sql(query, engine)

                market_dict = dict(zip(market_mapping["market_name"], market_mapping["market_id"]))
                order["Market"] = df["Market"].map(market_dict)

                query1 = 'SELECT "status_id", "status_name" FROM "OrderStatus";'
                status_mapping = pd.read_sql(query1, engine)

                status_dict = dict(zip(status_mapping["status_name"], status_mapping["status_id"]))
                order["Order Status"] = df["Order Status"].map(status_dict)

                orderAddress = order[["Order City", "Order State", "Order Country", "Order Region", "Market"]].drop_duplicates()

                cur = conn.cursor()

                order_address_data_insert_query = """
                INSERT INTO "OrderAddress" ("orderAdd_id", "orderAdd_city", "orderAdd_state", "orderAdd_country", "orderAdd_region", "orderAdd_market")
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                orderAdd_id = 1

                for index, row in orderAddress.iterrows():
                    cur.execute(order_address_data_insert_query, (orderAdd_id, row["Order City"], row["Order State"], row["Order Country"], row["Order Region"], row["Market"]))
                    orderAdd_id += 1
                    
                conn.commit()
                cur.close()
                conn.close()
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()

                # Add data to table 'Order'
                order_with_orderAdd_id = pd.merge(order, pd.read_sql('SELECT * FROM "OrderAddress"', engine),
                                                left_on=["Order City", "Order State", "Order Country", "Order Region", "Market"],
                                                right_on=["orderAdd_city", "orderAdd_state", "orderAdd_country", "orderAdd_region", "orderAdd_market"],
                                                how="left")

                # new DataFrame 
                new_order_table = order_with_orderAdd_id[["Order Id", "order date (DateOrders)", "Order Item Cardprod Id", "Order Item Quantity", 
                                                        "Customer Id", "Department Id", "Order Item Total", "Order Status", "orderAdd_id"]].copy()
                new_order_table.columns = ["order_id", "order_date", "prod_id", "quantity", "cust_id", "department_id", 
                                        "TotalAmount", "order_status", "orderAdd_id"]

                # Add column 'row_id' as an identifier
                new_order_table.insert(0, "row_id", range(1, len(new_order_table) + 1))

                new_order_table.to_sql('Order', engine, if_exists='append', index=False)

                print("Data inserted into tables Order, OrderAddress successfully!")   
        
        ## Insert data to table Shipping
        def insert_data_to_tables_6(df):
            try:
                conn = psycopg2.connect(
                        dbname= db,
                        user= user,
                        password= password,
                        host= host,
                        port= port
                    )           
                
                shipping = df[["Order Id", "Days for shipping (real)", "Days for shipment (scheduled)", "Delivery Status",
                            "shipping date (DateOrders)", "Shipping Mode"]]
                # mapping data to get status_id and shipMode_id
                query = 'SELECT "status_id", "status_name" FROM "ShippingStatus";'
                status_mapping = pd.read_sql(query, engine)

                status_dict = dict(zip(status_mapping["status_name"], status_mapping["status_id"]))
                shipping["Delivery Status"] = df["Delivery Status"].map(status_dict)

                query1 = 'SELECT "shipMode_id", "shipMode_name" FROM "ShippingMode";'
                mode_mapping = pd.read_sql(query1, engine)

                mode_dict = dict(zip(mode_mapping["shipMode_name"], mode_mapping["shipMode_id"]))
                shipping["Shipping Mode"] = df["Shipping Mode"].map(mode_dict)

                cur = conn.cursor()

                shipping_data_insert_query = """
                INSERT INTO "Shipping" ("ship_id", "order_id", "ship_date", "days_real", "days_scheduled", "ship_status", "ship_mode")
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                ship_id = 1

                for index, row in shipping.iterrows():
                    
                    if index > 0 and row["Order Id"] != shipping.iloc[index - 1]["Order Id"]:
                        ship_id += 1
                    
                    cur.execute(shipping_data_insert_query, (ship_id, row["Order Id"], row["shipping date (DateOrders)"], row["Days for shipping (real)"], 
                                                            row["Days for shipment (scheduled)"], row["Delivery Status"], row["Shipping Mode"]))
                    
                conn.commit()
                cur.close()
                conn.close()
                print("Data inserted into tables Shipping successfully!")
            except psycopg2.Error as e:
                print("Error: ", e)
            finally:
                if conn is not None:
                    conn.close()

        insert_data_to_tables_1(df), insert_data_to_tables_2(df), insert_data_to_tables_3(df)
        insert_data_to_tables_4(df), insert_data_to_tables_5(df), insert_data_to_tables_6(df)

    # Create tables
    execute_queries(create_tables_queries)

    # Create relationships
    execute_queries(create_relationships_queries)

    # Insert data
    insert_data_to_tables(df)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data from CSV-file to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)