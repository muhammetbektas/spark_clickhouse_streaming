import logging
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from clickhouse_driver import Client



def create_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS default.news
                   (source_id UUID,
                    source_name String,
                    author String,
                    title String,
                    description String,
                    url String,
                    urlToImage String,
                    publishedAt String,
                    content String)
                ENGINE = MergeTree
                ORDER BY source_id;""")
    print("Table created successfully")



def create_spark_connection():
    # creating spark connection
    packages = [
    "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.1",
    "com.clickhouse:clickhouse-client:0.7.2",
    "com.clickhouse:clickhouse-http-client:0.7.2",
    "org.apache.httpcomponents.client5:httpclient5:5.4.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.clickhouse:clickhouse-jdbc:0.7.1"]

    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config("spark.jars.packages", ",".join(packages)) \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers','broker:9092') \
            .option('subscribe','news_data') \
            .option('startingOffsets','earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        print("Kafka dataframe created successfully")
    except Exception as e:
        print(f"kafka dataframe could not be created because: {e}")
        
    return spark_df

def create_clickhouse_connection():
    #connecting to the clickhouse
    session = None
    try:
        session = Client(user="default", password="", host="clickhouse", port=9000)
    except Exception as e:
        logging.error(f"could not create clickhouse connection due to {e}")
        return None       
    return session

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("source_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("author", StringType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), False),
        StructField("url", StringType(), False),
        StructField("urlToImage", StringType(), False),
        StructField("publishedAt", StringType(), False),
        StructField("content", StringType(), False)
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print("Selection dataframe created successfully")
    return sel

# Write to ClickHouse
def write_to_clickhouse(batch_df,batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "news") \
        .option("user", "default") \
        .option("password", "") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()

    if spark_conn  is not None:
        #connect to kafka spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        session = create_clickhouse_connection()

        if session is not None:
            create_table(session)
            print("Streaming is being started...")
            streaming_query = selection_df.writeStream \
                                .foreachBatch(write_to_clickhouse) \
                                .outputMode("append") \
                                .start()

            streaming_query.awaitTermination() 