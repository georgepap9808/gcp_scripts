from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from google.cloud import storage
import sys

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts2")
blob = bucket.blob("spark_config_iceberg.py")
blob.download_to_filename("/tmp/spark_config_iceberg.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import spark session creation module
from spark_config_iceberg import create_spark_session

def parse_json_value(df):
    """Parse the JSON value column into individual fields"""

    # Define schema to match exactly with the producer schema
    json_schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("consumption", DoubleType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("minute", StringType(), False),
        StructField("date", StringType(), False),
        StructField("key", StringType(), False)
    ])

    # Parse JSON value column, maintaining exact field names from producer
    parsed_df = df.withColumn("parsed_value",
                              F.from_json(F.col("value"), json_schema)) \
        .select(
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        "parsed_value.*"
    )

    return parsed_df

def process_kafka_batch(spark, df, batch_id, db_schema, table_name):
    """Process a batch of Kafka data and write to Iceberg"""
    try:
        print(f"Processing batch {batch_id} for table {db_schema}.{table_name}")

        # Get table location path
        table_path = f"gs://osd-data2/{db_schema}.db/{table_name}"

        # Cast Kafka message format
        kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

        # Parse JSON values
        parsed_df = parse_json_value(kafka_df)

        # Add processing metadata
        final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
            .withColumn("batch_id", F.lit(batch_id))

        print("Preview of parsed data:")
        final_df.show(5, truncate=False)
        print("Schema of parsed data:")
        final_df.printSchema()

        # Create schema if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_schema}")

        # Create table if it doesn't exist (for first batch)
        if batch_id == 0:
            spark.sql(f"DROP TABLE IF EXISTS {db_schema}.{table_name}")

            create_table_sql = f"""
            CREATE TABLE {db_schema}.{table_name} (
                {', '.join([f"{col} {str(dtype).replace('Type','')}"
                            for col, dtype in final_df.dtypes])}
            )
            USING iceberg
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.object-storage.enabled' = 'true',
                'write.data.path' = '{table_path}/data',
                'write.metadata.path' = '{table_path}/metadata',
                'format-version' = '2',
                'write.metadata.delete-after-commit.enabled' = 'true',
                'write.metadata.previous-versions-max' = '5'
            )
            """
            spark.sql(create_table_sql)

        # Write batch to Iceberg
        print(f"Writing batch to Iceberg table at: {table_path}")
        final_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"{db_schema}.{table_name}")

        print(f"Successfully wrote batch {batch_id} to {table_path}")

        # Print batch statistics
        print("Batch Statistics:")
        final_df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("uuid").alias("unique_devices"),
            F.round(F.avg("consumption"), 2).alias("avg_consumption"),
            F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm:ss").alias("earliest_event"),
            F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm:ss").alias("latest_event")
        ).show(truncate=False)

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        raise

def verify_table_results(spark, db_schema, table_name):
    """Verify table results with proper error handling"""
    try:
        table_exists = (spark.sql(f"SHOW TABLES IN {db_schema}")
                        .filter(F.col("tableName") == table_name)
                        .count() > 0)

        if not table_exists:
            print(f"No data was processed. Table {db_schema}.{table_name} does not exist.")
            return

        result = spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT uuid) as unique_devices,
                ROUND(AVG(consumption), 2) as avg_consumption,
                MIN(ts) as earliest_event,
                MAX(ts) as latest_event,
                MIN(processing_time) as first_processed,
                MAX(processing_time) as last_processed
            FROM {db_schema}.{table_name}
        """)
        print("Final table statistics:")
        result.show(truncate=False)

    except Exception as e:
        print(f"Error verifying results: {str(e)}")
        raise

def main():
    try:
        # Initialize Spark session
        print("Creating Spark session...")
        spark = create_spark_session()

        # Define schema and table names
        db_name = "kafka_iceberg"
        table_name = "iot_events"

        # Clean up existing table
        table_loc = f"gs://osd-data2/{db_name}.db/{table_name}"
        spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")

        print("Setting up Kafka stream...")
        # Read from Kafka stream with exact configurations from template
        df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
            .option("subscribe", "osds-topic") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("Starting stream processing...")
        # Process the stream
        query = df_kafka.writeStream \
            .foreachBatch(lambda df, batch_id: process_kafka_batch(
            spark, df, batch_id, db_name, table_name
        )) \
            .outputMode("append") \
            .option("checkpointLocation", f"gs://osd-data2/checkpoints/{db_name}/{table_name}") \
            .trigger(processingTime='1 minute') \
            .start()

        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()

        # Verify results after stream completion
        verify_table_results(spark, db_name, table_name)

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()
