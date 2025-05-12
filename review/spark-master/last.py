import time
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, from_json, current_timestamp, when, lit,expr, lit
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
from TextCleaner import TextCleaner
from uuid import uuid4
import uuid

# Initialize Spark with Kafka integration
spark = SparkSession.builder \
    .appName("ReviewAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.jars", "/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.1.0.jar") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.streaming.unsupportedOperationCheck", "false") \
    .config("spark.cassandra.output.ignoreNulls", "true") \
    .getOrCreate()

# Load your model with error handling
try:
    model = joblib.load('/tmp/pipeline.pkl')
    model_broadcast = spark.sparkContext.broadcast(model)
except Exception as e:
    print(f"Failed to load model: {str(e)}")
    spark.stop()
    exit(1)

# Define schema for incoming Kafka messages
schema = StructType([
    StructField("asin", StringType()),
    StructField("review_text", StringType())
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
    when(col("data.review_text").isNull(), lit("")).otherwise(col("data.review_text")).alias("review_text")
).withColumn(
    "processing_time", current_timestamp()
).filter(
    (col("asin") != "") & (col("review_text") != "")
)

# Define prediction UDF
@pandas_udf(StringType())
def predict_udf(text_series: pd.Series) -> pd.Series:
    try:
        model = model_broadcast.value
        text_series = text_series.fillna("").astype(str)
        # print("*"*50)
        # temp = text_series.to_numpy()
        # for x in temp:
        #     print("="*15,end='')
        #     print(x)
        # print("*"*50)
        predictions = model.predict(text_series)
        return pd.Series(predictions)
    except Exception as e:
        print(f"Prediction error in batch: {str(e)}")
        return pd.Series([""] * len(text_series))

result_df = parsed_df.withColumn(
    "prediction", 
    when(
        (col("review_text") != ""),
        predict_udf(col("review_text"))
    ).otherwise(lit(""))
    ).withColumn(
    "id", expr("uuid()")  # Spark's built-in UUID generator
    ).select(
        "id", "asin", "review_text", "prediction", "processing_time"
    )


# Add a debug stream before Cassandra write
debug_query = result_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

# Configure Cassandra writer with error tolerance
try:
    query = result_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "review_analysis") \
        .option("table", "product_reviews") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
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