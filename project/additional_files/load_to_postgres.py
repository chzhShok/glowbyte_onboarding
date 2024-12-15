import csv
import psycopg2
from tqdm import tqdm

DB_HOST = "localhost"
DB_NAME = "data"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_PORT = "5432"  

TABLE_COUNTRY_NAME = "country_aliases" 
TABLE_CHARTS_NAME = "charts" 

CSV_COUNTRY_FILE_PATH = "country_aliases.csv"
CSV_CHARTS_FILE_PATH = "charts.csv"

try:
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cursor = connection.cursor()

    ## COUNTRY
    cursor.execute("DROP TABLE IF EXISTS country_aliases")
    cursor.execute("CREATE TABLE country_aliases (alias TEXT, country TEXT)")
    with open(CSV_COUNTRY_FILE_PATH, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок CSV
        for row in tqdm(reader):
            cursor.execute(
                f"""
                INSERT INTO {TABLE_COUNTRY_NAME} 
                (alias, country) 
                VALUES (%s, %s)
                """,
                (
                    row[0],                      # date
                    row[1],                      # country
                )
            )
    
    connection.commit()
    print("Данные country_aliases успешно добавлены в таблицу PostgreSQL")

    ## CHARTS
    cursor.execute("DROP TABLE IF EXISTS charts")
    cursor.execute("""
    CREATE TABLE charts (
        date DATE,   
        country TEXT,
        position INT,
        streams INT, 
        track_id TEXT,
        artists TEXT,       
        artist_genres TEXT, 
        duration INT,
        explicit BOOLEAN,     
        name TEXT 
    )
    """)
    with open(CSV_CHARTS_FILE_PATH, 'r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in tqdm(reader):
            cursor.execute(
                f"""
                INSERT INTO {TABLE_CHARTS_NAME} (
                    date, 
                    country,
                    position, 
                    streams, 
                    track_id, 
                    artists, 
                    artist_genres, 
                    duration,
                    explicit, 
                    name
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row[0],         
                    row[1],    
                    int(row[2]),    
                    int(row[3]),         
                    row[4],         
                    row[5],         
                    row[6], 
                    int(row[7]),              
                    row[8].lower() == 'true',       
                    row[9]               
                )
            )
    
    # Подтверждение изменений
    connection.commit()
    print("Данные charts успешно добавлены в таблицу PostgreSQL")

except Exception as e:
    print(f"Ошибка: {e}")
    connection.rollback()  # Откат изменений в случае ошибки

finally:
    # Закрытие подключения
    if cursor:
        cursor.close()
    if connection:
        connection.close()
