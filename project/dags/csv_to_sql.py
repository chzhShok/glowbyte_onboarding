import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import csv
import logging
import glob
import os

def take_csv_path(dirs, pattern) -> list[str]:
    files = []  
    for dir_path in dirs:
        files.extend(glob.glob(os.path.join(dir_path, pattern)))
    
    return files


def create_tables(cursor):
    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS charts (
    #     date date,
    #     position bigint,
    #     streams bigint,
    #     track_id text,
    #     artists text,
    #     artist_genres text,
    #     explicit text,
    #     name text,
    #     duration_minutes real,
    #     country_name text
    # )
    # """)

    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS country_aliases (
    #     alias text,
    #     country_name text
    # )
    # """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_artists_by_country (
        country_name text,
        artist text,
        total_streams real
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_genres_by_country (
        country_name text,
        genre text,
        genre_count bigint
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_songs_by_countries (
        country_name text,
        name text
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_songs_in_more_than_1_country (
        name text,
        artists text,
        total_countries bigint
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_songs_more_than_1_time (
        name text,
        artists text
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS total_and_max_streams_by_country (
        country_name text,
        total_streams real,
        max_stream bigint,
        count_songs bigint
    )
    """)


def write_to_tables(cursor, connection, tables_files: dict[str, list[str]]):
    for table, files in tables_files.items():
        for file_path in files:
            if not os.path.exists(file_path):
                logging.warning(f"CSV file not found: {file_path}")
                continue

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    reader = csv.reader(file)
                    
                    # Читаем заголовки
                    try:
                        headers = next(reader)
                    except StopIteration:
                        logging.error(f"Empty CSV file: {file_path}")
                        continue

                    # Подготавливаем запрос
                    placeholders = ", ".join(["%s"] * len(headers))
                    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                        table=sql.Identifier(table),
                        fields=sql.SQL(", ").join(map(sql.Identifier, headers)),
                        values=sql.SQL(placeholders)
                    )

                    # Читаем строки
                    rows = list(reader)
                    if not rows:
                        logging.warning(f"No data in file: {file_path}")
                        continue
                    
                    try:
                        execute_batch(cursor, insert_query, rows)

                        connection.commit()
                        logging.info(f"Data successfully inserted into {table} from {file_path}")

                    except Exception as e:
                        logging.error(f"Error while inserting data from {file_path} into {table}: {e}")
                        connection.rollback()

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")


def csv_to_sql():
    tables = [
        'charts',
        'country_aliases',
        'top_artists_by_country',
        'top_genres_by_country',
        'top_songs_by_countries',
        'top_songs_in_more_than_1_country',
        'top_songs_more_than_1_time',
        'total_and_max_streams_by_country',
    ]

    connection = None
    cursor = None

    try:
        connection = psycopg2.connect(
                host="postgres",
                database="data",
                user="admin",
                password="admin",
                port="5432"
            )
        logging.info("Connected to postgres")

        cursor = connection.cursor()

        for table in tables:
            cursor.execute('DROP TABLE IF EXISTS ' + str(table))
        logging.info("Dropped tables")

        base_path = '/opt/airflow/prepared_csv/*.csv'
        pattern = '*.csv'

        dirs = glob.glob(base_path)
        files = take_csv_path(dirs, pattern)

        create_tables(cursor)
        connection.commit()
        logging.info("Created tables")

        tables_files = {table: [] for table in tables}

        for file in files:
            file_psedo = file.split('/')[4].split('.')[0]
            tables_files[file_psedo].append(file)
    
        write_to_tables(
            cursor=cursor,
            connection=connection,
            tables_files=tables_files,
        )
        connection.commit()
        logging.info("Wrote tables in Postgres")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
