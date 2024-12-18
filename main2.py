from confluent_kafka import Consumer, KafkaException
import json
from pymongo import MongoClient

def moving_average(c,price,ave):
    return (((c-1)*ave)+price)/count

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

client = MongoClient("mongodb://admin:password@localhost:27017/")
db = client["stock_data_db"]
collection = db["stock_prices"]

if __name__ == '__main__':
    try:
        consumer.subscribe([TOPIC])
        count = 0
        average = 0
        while True:
            count = count+1
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Deserialize and save to MongoDB
            stock_data = json.loads(msg.value().decode('utf-8'))
            average =moving_average(count,stock_data['price'],average)
            collection.insert_one(average)
            print("Data inserted into MongoDB")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        client.close()