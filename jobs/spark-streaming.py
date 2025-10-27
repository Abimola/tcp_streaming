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
            model = "gpt-5-nano",
            messages = [{
                "role": "system",
                "content": f"""
                            You are a sentiment classifier.
                            Classify the following comment as POSITIVE, NEGATIVE, or NEUTRAL.
                            Respond with only one word, exactly one of: POSITIVE, NEGATIVE, or NEUTRAL.

                            Here is the comment:
                            {comment}
                           """
                    },
                    {"role": "user", "content": comment}
               ]
        )
        return completion.choices[0].message.content.strip()
    else:
        return "Empty"



def start_streaming(spark):
    topic = "reviews"
    try: 
        stream_df = (spark.readStream.format("socket")
                    .option("host", "0.0.0.0")
                    .option("port", 9999)
                    .load()
                    )
        
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType())
        ])
        
        stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

        sentiment_analysis_udf = udf(sentiment_analysis, StringType())

        stream_df = stream_df.withColumn('feedback',
                                         when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                         .otherwise(None)
                                         )

        kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

        query = (kafka_df.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", config["kafka"]['bootstrap.servers'])
                .option("kafka.security.protocol", config['kafka']['security.protocol'])
                .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                .option("kafka.sasl.jaas.config",
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
        print(f'Exception encountered: {e}. Retrying in 5 seconds')
        sleep(5)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConumer").getOrCreate()

    start_streaming(spark_conn)