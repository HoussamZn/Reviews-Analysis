from confluent_kafka import Producer
import time
import json
import os
producer = Producer({'bootstrap.servers': 'kafka:29092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else: print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    with open("val_data.json", "r") as f:
        data = json.load(f)
        for record in data:
            producer.produce("product-reviews", value=json.dumps(record).encode("utf-8"), callback=delivery_report)
            producer.poll(0)
            print("message sent, sleepign for 1s.")
            time.sleep(1)
    producer.flush()

if __name__=="__main__":
    main()








# import json
# import logging
# import time
# from confluent_kafka import Producer, KafkaException
# from confluent_kafka.admin import AdminClient, NewTopic 
# from review_scraper import Scraper

# PRODUCT_LINKS = [
#     "https://www.amazon.com/dp/B09MLRPTT2?ref=emc_s_m_5_i_atc",
#     "https://www.amazon.com/SK-hynix-Platinum-Internal-Compact/dp/B09QVD9V7R?pd_rd_w=9qhOR&content-id=amzn1.sym.cd152278-debd-42b9-91b9-6f271389fda7&pf_rd_p=cd152278-debd-42b9-91b9-6f271389fda7&pf_rd_r=RQDDKZJ3KVX06QNEGRGT&pd_rd_wg=npTKQ&pd_rd_r=5d03d01c-600b-426a-95c5-bb57a95c2492&pd_rd_i=B09QVD9V7R",
#     "https://www.amazon.com/SAMSUNG-Internal-Expansion-MZ-V9P2T0B-AM/dp/B0BHJJ9Y77?_encoding=UTF8&content-id=amzn1.sym.860dbf94-9f09-4ada-8615-32eb5ada253a&dib=eyJ2IjoiMSJ9.YXOHQK4jgmiqqegAuE80pwfZKp6gPrEBXUHlrwGRq6Jb0vjwTvRQ9G1dXadgaDx9KoxlTaBnvxvUsYtKoKFB5iuVGij56WSLL7WxqMyEX1YltGDwGdfSqvwnISWxDIeUF8jR1IH2-d3lbVZ7RvqEJIuYwc4HDaDHXyifumxrFDumseo-71SQSF2e73eHaW9RTgyq-UEdEWkzE_bTcG-3IIU4SBluzlqSJ5OjLSAWLb7YeDmSti0cA9pEzHeksrCsJlGVfJ1rx0wiGryuG343nyjX3FIEqYUK3BOL52oSxOI.spIMutowPkAEHrTgcfpUOgO2MKDUIaYmw14OY6ELsxE&dib_tag=se&keywords=gaming&pd_rd_r=cc02d1d7-9ce7-4511-a860-cda15a2b7bab&pd_rd_w=aAHDJ&pd_rd_wg=KXjSq&qid=1746671633&sr=8-6",
#     "https://www.amazon.com/KTC-Computer-Monitor-Adaptive-Sync-H25B7/dp/B0DK3FKSW1/131-9061157-9579603?pd_rd_w=HfYDV&content-id=amzn1.sym.05a10d12-4813-462d-bcf3-afbc7ac69089&pf_rd_p=05a10d12-4813-462d-bcf3-afbc7ac69089&pf_rd_r=QFC959JZA5D6EY8GTBEC&pd_rd_wg=MdRBd&pd_rd_r=7301ca9f-f77d-4393-87fa-95c4ebb846e5&pd_rd_i=B0DK3FKSW1&psc=1",
#     ]

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger("amazon_review_scraper")

# # Kafka configuration
# KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
# KAFKA_TOPIC = 'product-reviews'
# DEFAULT_PARTITIONS = 1
# DEFAULT_REPLICATION_FACTOR = 1


# class KafkaReviewProducer:
#     def __init__(self):
#         self.scraper = Scraper()
#         self.producer = None
#         self.admin_client = None
#         self.running = False
#         self._setup_kafka()

#     def _setup_kafka(self):
#         """Set up Kafka producer."""
#         producer_config = {
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'client.id': 'amazon-review-scraper',
#             'compression.type': 'snappy',
#             'acks': 'all'
#         }
#         try:
#             self.producer = Producer(producer_config)
#             self.admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
#             logger.info("Kafka producer initialized successfully")
#         except KafkaException as e:
#             logger.error(f"Failed to initialize Kafka producer: {e}")
#             raise

#     def _delivery_report(self, err, msg):
#         if err is not None:
#             logger.error(f"Message delivery failed: {err}")
#         else:
#             logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

#     def _initialize_topic(self):
#         """Initialize the Kafka topic. Create it if it doesn't exist."""
#         try:
#             metadata = self.admin_client.list_topics(timeout=10)
#             if KAFKA_TOPIC not in metadata.topics:
#                 logger.info(f"Topic {KAFKA_TOPIC} does not exist. Creating it...")
#                 new_topic = NewTopic(
#                     topic=KAFKA_TOPIC,
#                     num_partitions=DEFAULT_PARTITIONS,
#                     replication_factor=DEFAULT_REPLICATION_FACTOR
#                 )
#                 self.admin_client.create_topics([new_topic])
#                 logger.info(f"Topic {KAFKA_TOPIC} created successfully")
#             else:
#                 logger.info(f"Topic {KAFKA_TOPIC} already exists")
#         except KafkaException as e:
#             logger.error(f"Failed to initialize or create topic {KAFKA_TOPIC}: {e}")
#             raise

#     def send_review(self, review):
#         """Send a review to Kafka."""
#         try:
#             review_json = json.dumps(review)
#             self.producer.produce(
#                 topic=KAFKA_TOPIC,
#                 key=review.get('ASIN', 'unknown'),
#                 value=review_json,
#                 callback=self._delivery_report
#             )
#             self.producer.poll(0)
#         except Exception as e:
#             logger.error(f"Error sending review to Kafka: {e}")

#     def start(self):
#         self.running = True
#         while self.running:
#             try:
#                 self._initialize_topic()  # Ensure topic is initialized before sending
#                 for url in PRODUCT_LINKS:
#                     logger.info(f"Scraping reviews for product URL: {url}")

#                     result = self.scraper.get_reviews(url)
#                     if result:
#                         message = dict() # Instance of the message to send to kafka topic {"ASIN":"", "review":""}
#                         message["asin"] = result["ASIN"]

#                         for review in result["reviews"]:
#                             message['review_text']=review["review_text"]
#                             print("******************************************************************************************************************************************************")
#                             print(message)
#                             print("******************************************************************************************************************************************************")
#                             self.send_review(message)
#                     else:
#                         logger.warning(f"No reviews found for URL: {url}")

#                 time.sleep(5)  # Wait for 5 seconds before checking again

#             except Exception as e:
#                 logger.error(f"Error in producer loop: {e}")
#                 self.stop()

#     def stop(self):
#         """Stop the producer."""
#         self.running = False
#         if self.producer:
#             self.producer.flush(timeout=10)
#             logger.info("Producer stopped and flushed")

#     def close(self):
#         """Flush and close the producer."""
#         remaining = self.producer.flush(timeout=10)
#         if remaining > 0:
#             logger.warning(f"{remaining} messages were not delivered")


# def main():
#     try:
#         kafka_producer = KafkaReviewProducer()
#         kafka_producer.start()
#     except Exception as e:
#         logger.error(f"Error in main: {e}")
#     finally:
#         kafka_producer.stop()
#         kafka_producer.close()


# if __name__ == "__main__":
#     main()


# # structure of the scraped reviews
# # results = {
# #     "ASIN": "ASIN-CODE",
# #     "reviews": [
# #         {
# #             "customer_name": "",
# #             "review_date": "",
# #             "review_text": "",
# #             "review_title": ""
# #         },
# #         {
# #             "customer_name": "",
# #             "review_date": "",
# #             "review_text": "",
# #             "review_title": ""
# #         },
# #         # ...
# #     ]
# # }
    