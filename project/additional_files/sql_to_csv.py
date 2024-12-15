import psycopg2
import csv
import os
import logging

def sql_to_csv():
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

        cursor = connection.cursor()

        csv_path = "/opt/airflow/csv"

        if not os.path.exists(csv_path):
            os.makedirs(csv_path)

        for table in ['country_aliases', 'charts']:
            with open(f"{csv_path}/{table}.csv", 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)

                cursor.execute(f"select * from {table} limit 0")

                column_names = [desc[0] for desc in cursor.description]

                writer.writerow(column_names)

                offset = 0
                # max_offset = 2_000_000
                max_offset = 2_000
                batch_size = 10_000
                while (offset <= max_offset):
                    cursor.execute(f"select * from {table} offset %s limit %s", (offset, batch_size))
                    rows = cursor.fetchall()

                    if not rows: 
                        break

                    writer.writerows(rows)

                    offset += batch_size
                    logging.info(f"Processed {offset} rows")

            logging.info(f"Data wrote in {table}.csv")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
