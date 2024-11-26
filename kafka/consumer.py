import json

from confluent_kafka import Consumer
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

TOPIC = 'sales-history'

config = {
    'bootstrap.servers': 'localhost:8087',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

consumer = Consumer(config)
consumer.subscribe([TOPIC])
print(f"Subscribed to topic: {TOPIC}")

connection = psycopg2.connect(
    database="kafka",
    user="kafka",
    password="kafka",
    host="localhost",
    port=5432
)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = connection.cursor()

cursor.execute("CREATE table if not exists sales ("
               "id serial PRIMARY KEY, "
               "product varchar(255) not null, "
               "date date not null, "
               "created_at date not null default current_date"
")")

insert_query = "INSERT INTO sales (product, date) VALUES (%s, %s)"

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
        else:
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {data['product'], data['date']}")

            cursor.execute(insert_query, (data['product'], data['date']))
            print("Save to Postgres")
except KeyboardInterrupt:
    print("Consumer stopped")
finally:
    consumer.close()
