from confluent_kafka import Producer
import time
import json
producer = Producer({'bootstrap.servers': 'kafka:29092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else: print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    with open("test_data.json", "r") as f:
        data = json.load(f)
        for record in data:
            print("data : " ,record)
            producer.produce("product-reviews", value=json.dumps(record).encode("utf-8"), callback=delivery_report)
            producer.poll(0)
            print("message sent, sleepign for 1s.")
            time.sleep(1)
    producer.flush()

if __name__=="__main__":
    main()