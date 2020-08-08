import configparser
import psycopg2
from sql_queries import create_schema_queries, \
    create_staging_table_queries, create_dwh_table_queries, \
    drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_staging_tables(cur, conn):
    cur.execute('SET search_path TO staging;')
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()


def create_dwh_tables(cur, conn):
    cur.execute('SET search_path TO dwh;')
    for query in create_dwh_table_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur, conn):
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # print(*config['CLUSTER'].values())

    conn = \
        psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(*config['CLUSTER'
                         ].values()))
    cur = conn.cursor()
    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_staging_tables(cur, conn)
    create_dwh_tables(cur, conn)

    conn.close()


if __name__ == '__main__':
    main()
