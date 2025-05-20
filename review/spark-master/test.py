from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp, when, lit
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml import PipelineModel
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('punkt_tab') 
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def preprocess_text(text):
    tokens = nltk.word_tokenize(text)
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word.isalpha() and word not in stop_words]
    return ' '.join(tokens)


# Initialize Spark with Kafka integration
spark = SparkSession.builder \
    .appName("ReviewAnalysis") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,"
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.sql.streaming.unsupportedOperationCheck", "false") \
    .getOrCreate()
 
# Load your model with error handling
try:
    model = PipelineModel.load("/review/models")
except Exception as e:
    print(f"Failed to load model: {str(e)}")
    spark.stop()
    exit(1)

# Define schema for incoming Kafka messages
schema = StructType([
    StructField("asin", StringType()),
    StructField("reviewText", StringType())
])

# Create streaming DataFrame from Kafka with error handling
try:
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "product-reviews") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "latest") \
        .load()
except Exception as e:
    print(f"Failed to create Kafka stream: {str(e)}")
    spark.stop()
    exit(1)

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    when(col("data.asin").isNull(), lit("")).otherwise(col("data.asin")).alias("asin"),
    when(col("data.reviewText").isNull(), lit("")).otherwise(col("data.reviewText")).alias("reviewText")
).withColumn(
    "processing_time", current_timestamp()
).filter(
    (col("asin") != "") & (col("reviewText") != "")
)


clean_text_udf = udf(clean_text, StringType())
preprocess_text_udf = udf(preprocess_text, StringType())

processed_df = parsed_df.withColumn("reviews", col("reviewText")) \
    .withColumn("cleaned_reviews", clean_text_udf("reviews")) \
    .withColumn("processed_reviews", preprocess_text_udf("cleaned_reviews"))


prediction_df = model.transform(processed_df)

# Convert numeric prediction to label
sentiment_labels = {0: "Negative", 1: "Neutral", 2: "Positive"}
label_udf = udf(lambda x: sentiment_labels.get(int(x), "Unknown"), StringType())

result_df = prediction_df.withColumn("prediction", label_udf(col("prediction"))) \
    .select("asin", "reviewText", "prediction", "processing_time")


# Add a debug stream before Cassandra write
debug_query = result_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

# Configure mongoDB writer with error tolerance
try:
    def write_to_mongo(batch_df, batch_id):
        batch_df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", "mongodb://mongo:27017/review_analysis.product_reviews") \
            .save()

    query = result_df.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/mongo-checkpoint") \
        .trigger(processingTime='5 seconds') \
        .start()
except Exception as e:
    print(f"Failed to start streaming query: {str(e)}")
    spark.stop()
    exit(1)

try:
    query.awaitTermination()
except Exception as e:
    print(f"Streaming query terminated with error: {str(e)}")
    print("Attempting to restart...")
    try:
        query.start().awaitTermination()
    except Exception as e:
        print(f"Failed to restart streaming: {str(e)}")
finally:
    spark.stop()