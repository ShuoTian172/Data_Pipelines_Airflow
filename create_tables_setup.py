import configparser
import psycopg2
from sql_queries_setup import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function can be used to drop tables we create in database.

    Arguments:
        cur: the cursor object. 
        coon: this connection object. 

    Returns:
    None
    """
        
    print('start droping' + '='*10 + '>')
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function can be used to create table from queries in database.

    Arguments:
        cur: the cursor object. 
        coon: this connection object. 

    Returns:
    None
    """
    print('start creating' + '='*10 + '>')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function can be used to set up the environment we need.

    Arguments:
    None
    
    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port= {}".format(*config['CLUSTER'].values())) 
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()