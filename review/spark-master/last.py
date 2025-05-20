import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, from_json, current_timestamp, when, lit,expr, lit
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
from TextCleaner import TextCleaner

# Initialize Spark with Kafka integration
spark = SparkSession.builder \
    .appName("ReviewAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.sql.streaming.unsupportedOperationCheck", "false") \
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
    "processingTime", current_timestamp()
).filter(
    (col("asin") != "") & (col("reviewText") != "")
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
        (col("reviewText") != ""),
        predict_udf(col("reviewText"))
    ).otherwise(lit(""))
    ).select(
        "asin", "reviewText", "prediction", "processingTime"
    )


# Add a debug stream before Cassandra write
debug_query = result_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime='1 seconds') \
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
        .trigger(processingTime='1 seconds') \
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