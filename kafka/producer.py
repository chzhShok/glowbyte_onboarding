import json
import random
from datetime import datetime, timedelta
from time import sleep

from confluent_kafka import Producer, KafkaError

TOPIC = 'sales-history'
START_DATE = datetime(2024, 1, 1, 0, 0, 0)
END_DATE = datetime(2024, 12, 31, 23, 59, 59)
MID_OF_YEAR = START_DATE + (END_DATE - START_DATE) / 2
products = [
    "Apple", "Banana", "Orange", "Grape", "Mango",
    "Pear", "Cherry", "Strawberry", "Watermelon", "Lemon",
    "Tomato", "Cucumber", "Carrot", "Potato", "Onion",
    "Broccoli", "Spinach", "Kale", "Avocado", "Peach",
    "Plum", "Apricot", "Blueberry", "Raspberry", "Kiwi",
    "Pineapple", "Melon", "Fig", "Pomegranate"
]

config = {
    'bootstrap.servers': 'localhost:8087',
    'compression.type': 'gzip',
    'enable.idempotence': True,
}

producer = Producer(config)


def callback_message(err, msg):
    if err:
        print(
            f"Message delivery failed for topic {msg.topic()} "
            f"[{msg.partition()}]: {err}")
        if err.code() == KafkaError._MSG_TIMED_OUT:
            print("Error: Message timed out while trying to deliver.")
        elif err.code() == KafkaError._ALL_BROKERS_DOWN:
            print("Error: All brokers are down, cannot deliver message.")
        elif err.code() == KafkaError.REQUEST_TIMED_OUT:
            print("Error: Request to broker timed out.")
        else:
            print(f"Unhandled error code: {err.code()}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] "
            f"at offset {msg.offset()}")


def choose_partition(data: tuple[str, datetime]) -> int:
    if data[1] > MID_OF_YEAR:
        return 1
    return 0


def to_json(data: dict) -> str:
    return json.dumps(data)


def generate_random_date() -> datetime:
    delta = END_DATE - START_DATE
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return START_DATE + timedelta(seconds=random_second)


def generate_data_and_partition():
    product = random.choice(products)
    date = generate_random_date()

    partition = choose_partition((product, date))

    dict_data = {
        'product': product,
        'date': date.strftime("%Y-%m-%d"),
    }
    return to_json(dict_data), partition


try:
    while True:
        data, partition = generate_data_and_partition()

        producer.produce(
            topic=TOPIC,
            value=data,
            callback=callback_message,
            partition=partition,
        )

        print(f"Send message [{data}]")
        producer.flush()
        sleep(5)
except KeyboardInterrupt:
    print("Producer stopped")
