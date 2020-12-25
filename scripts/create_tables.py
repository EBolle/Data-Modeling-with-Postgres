from settings import username, password
from src.data_utils import create_database
from src.sql_queries import create_table_queries, drop_table_queries


def main():
    """
    - drops (if exists) and creates the sparkify database
    - establishes connection with the sparkify database and gets cursor to it
    - drops all the tables.
    - creates all tables needed.
    - closes the connection.
    """
    cur, conn = create_database(username, password)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


if __name__ == "__main__":
    print("creating the sparkify database...")
    main()
    print("done")
