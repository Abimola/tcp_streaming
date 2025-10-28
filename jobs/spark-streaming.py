from time import sleep
import pyspark
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import config


def sentiment_analysis(comment) -> str:
    if comment:
        client = OpenAI(api_key=config['openai']['api_key'])
        completion = client.chat.completions.create(
            model="gpt-5-nano",
            messages=[
                {
                    "role": "system",
                    "content": """
                        You are a sentiment classifier.
                        Classify the following comment as POSITIVE, NEGATIVE, or NEUTRAL.
                        Respond with only one word, exactly one of: POSITIVE, NEGATIVE, or NEUTRAL.
                    """
                },
                {"role": "user", "content": comment}
            ]
        )
        return completion.choices[0].message.content.strip()
    else:
        return "Empty"


def start_streaming(spark):
    topic = "streaming_reviews"
    try:
        # Read text stream from socket source
        stream_df = (
            spark.readStream.format("socket")
            .option("host", "0.0.0.0")
            .option("port", 9999)
            .load()
        )

        # Define schema for structured parsing of incoming JSON
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType())
        ])

        # Parse JSON stream into structured DataFrame
        stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select("data.*")

        # Register UDF for sentiment classification
        sentiment_analysis_udf = udf(sentiment_analysis, StringType())

        # Apply sentiment analysis to each incoming text
        stream_df = stream_df.withColumn(
            'feedback',
            when(col('text').isNotNull(), sentiment_analysis_udf(col('text'))).otherwise(None)
        )

        # Prepare data for Kafka output
        kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

        # Write processed data stream to Kafka topic
        query = (
            kafka_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config["kafka"]['bootstrap.servers'])
            .option("kafka.security.protocol", config['kafka']['security.protocol'])
            .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
            .option(
                "kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{config["kafka"]["sasl.username"]}" '
                f'password="{config["kafka"]["sasl.password"]}";'
            )
            .option('checkpointLocation', './checkpoint')
            .option('topic', topic)
            .start()
            .awaitTermination()
        )

    except Exception as e:
        # Retry logic for streaming or connection failure
        print(f'Exception encountered: {e}. Retrying in 5 seconds')
        sleep(5)


if __name__ == "__main__":
    # Initialize Spark session and start streaming job
    spark_conn = SparkSession.builder.appName("SocketStreamConumer").getOrCreate()
    start_streaming(spark_conn)
