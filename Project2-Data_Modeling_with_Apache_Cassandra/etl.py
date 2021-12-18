import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_count_rows_tables


def load_staging_tables(cur, conn):
    # Function that load the event and songs files into staging tables
    # Function name: load_staging_tables
    # Parameters: conn for database connection
    #             cur   
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # Function that load the data into fact and dimension tables
    # Function name: insert_tables
    # Parameters: conn for database connection
    #             cur  
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def count_rows(cur, conn):
    # Function that print the number of rows stored in each table
    # Function name: count_rows
    # Parameters: conn for database connection
    #             cur  
    for query in select_count_rows_tables:

        cur.execute(query)
        results = cur.fetchone()
        table_name = query.split()

        for row in results:
            print("Total score for {} is {}".format(table_name[5], row))
            

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e.pgerror)
        print(e.diag.message_detail)

    print("Connected!")
     
    try:
        cur = conn.cursor()
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        print("***********************************************")
        count_rows(cur, conn)
        conn.close()
    except psycopg2.Error as e:
        print(e)

if __name__ == "__main__":
    main()