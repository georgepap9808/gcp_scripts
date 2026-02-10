"""
Kafka CSV Batch Producer - Generate Large CSV Datasets and Send to Kafka

This script generates a large CSV dataset (default 5 million rows) and sends it to Kafka
in CSV format for batch streaming ingestion into data lake formats.

Usage:
    python kafka_csv_batch_producer.py --rows 5000000 --topic batch-csv-topic --format delta
    python kafka_csv_batch_producer.py --rows 5000000 --topic batch-csv-topic --format hudi --batch-size 10000
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys
from datetime import datetime, timedelta
import random
import time
import argparse
import builtins  # For built-in min/max functions

# Try to use local spark_config_delta.py first (for local development)
script_dir = os.path.dirname(os.path.abspath(__file__))
local_config = os.path.join(script_dir, "spark_config_delta.py")

if os.path.exists(local_config):
    # Use local config file
    sys.path.insert(0, script_dir)
    from spark_config_delta import create_spark_session
else:
    # Try to download from GCS (for Kubernetes/production)
    try:
        # Try to use service account key if available (Kubernetes environment)
        if os.path.exists("/mnt/secrets/key.json"):
            client = storage.Client.from_service_account_json("/mnt/secrets/key.json")
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            client = storage.Client.from_service_account_json(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        else:
            # Try to use default credentials
            client = storage.Client()
        bucket = client.get_bucket("osd-scripts2")
        blob = bucket.blob("spark_config_delta.py")
        blob.download_to_filename("/tmp/spark_config_delta.py")
        sys.path.insert(0, '/tmp')
        from spark_config_delta import create_spark_session
    except Exception as e:
        print(f"Error: Could not download spark_config_delta.py from GCS: {str(e)}")
        print("Please ensure spark_config_delta.py is available locally or set up GCP credentials")
        print("For local development, place spark_config_delta.py in the same directory as this script")
        sys.exit(1)


def generate_csv_data(spark, num_rows, format_type='delta'):
    """
    Generate a large DataFrame with CSV-compatible IoT event data.
    
    Args:
        spark: SparkSession
        num_rows: Number of rows to generate
        format_type: Data lake format (for schema customization if needed)
    
    Returns:
        DataFrame with IoT event schema
    """
    print(f"Generating {num_rows:,} rows of CSV data...")
    
    # Use Spark's native data generation capabilities for large datasets
    # Generate base data using range and then transform
    base_time = datetime.now()
    
    # Generate data in partitions for better performance
    # Use builtins.min/max to avoid conflict with PySpark functions
    import builtins
    partitions = builtins.max(100, builtins.min(200, num_rows // 50000))  # 100-200 partitions
    
    data_schema = StructType([
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
    
    # Use RDD for efficient large-scale data generation
    def generate_partition_rows(partition_idx):
        """Generate rows for a partition - optimized for large datasets"""
        rows_per_part = num_rows // partitions
        start_idx = partition_idx * rows_per_part
        end_idx = builtins.min(start_idx + rows_per_part, num_rows)
        
        for i in range(start_idx, end_idx):
            # Generate unique device IDs
            device_num = (i % 100) + 1  # 100 unique devices
            device_id = f"IoT_{device_num:03d}"
            
            # Generate timestamps with variation (1 minute intervals)
            seconds_offset = i * 60
            event_time = base_time - timedelta(seconds=seconds_offset)
            
            # Generate consumption values (40.0 to 50.0)
            consumption = round(40.0 + (i % 1000) / 100.0, 2)
            
            yield (
                device_id,
                event_time,
                consumption,
                str(event_time.month),
                str(event_time.day),
                str(event_time.hour),
                str(event_time.minute),
                event_time.strftime("%Y/%m/%d"),
                f"{device_id}_{event_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
    
    # Create RDD using mapPartitions for efficiency
    rdd = spark.sparkContext.parallelize(range(partitions), partitions) \
        .flatMap(generate_partition_rows)
    
    # Convert RDD to DataFrame
    df = spark.createDataFrame(rdd, data_schema)
    
    print(f"Generated {df.count():,} rows")
    return df


def df_to_csv_string(df):
    """
    Convert DataFrame to CSV string format for Kafka.
    Each row becomes a CSV string value in Kafka message.
    
    Args:
        df: Spark DataFrame
    
    Returns:
        DataFrame with CSV string values
    """
    # Convert all columns to strings and create CSV row
    csv_cols = [F.col(col).cast(StringType()).alias(col) for col in df.columns]
    df_strings = df.select(*csv_cols)
    
    # Create CSV string: join columns with comma
    # Handle nulls and escape commas/quotes if needed
    csv_expr = F.concat_ws(",", *[F.coalesce(F.col(c), F.lit("")) for c in df.columns])
    
    csv_df = df_strings.select(
        F.col("key").alias("kafka_key"),
        csv_expr.alias("csv_value")
    )
    
    return csv_df


def write_csv_to_kafka(df, bootstrap_servers, topic, batch_size=10000):
    """
    Write DataFrame as CSV strings to Kafka topic in batches.
    
    Args:
        df: Spark DataFrame with IoT event data
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic name
        batch_size: Number of rows per batch (for progress tracking)
    
    Returns:
        Total number of rows written
    """
    print(f"Converting DataFrame to CSV format...")
    
    # Convert to CSV strings
    csv_df = df_to_csv_string(df)
    
    # Cache the CSV DataFrame to avoid recomputation
    csv_df.cache()
    total_rows = csv_df.count()
    print(f"Total CSV rows to write: {total_rows:,}")
    
    # Write to Kafka
    print(f"Writing to Kafka topic: {topic}")
    print(f"Kafka servers: {bootstrap_servers}")
    
    start_time = time.time()
    
    # Write all data to Kafka
    # Each row becomes a Kafka message with CSV string as value
    csv_df.select(
        F.col("kafka_key").cast("string").alias("key"),
        F.col("csv_value").cast("string").alias("value")
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .save()
    
    elapsed_time = time.time() - start_time
    
    print(f"Successfully wrote {total_rows:,} CSV rows to Kafka in {elapsed_time:.2f} seconds")
    print(f"Throughput: {total_rows / elapsed_time:,.0f} rows/second")
    
    csv_df.unpersist()
    
    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description='Generate CSV data and send to Kafka for batch streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 5M rows and send to Kafka
  python kafka_csv_batch_producer.py --rows 5000000 --topic batch-csv-topic --format delta
  
  # Generate 1M rows with custom batch size
  python kafka_csv_batch_producer.py --rows 1000000 --topic batch-csv-topic --format hudi --batch-size 5000
  
  # Generate data for Iceberg format
  python kafka_csv_batch_producer.py --rows 5000000 --topic batch-csv-topic --format iceberg
        """
    )
    
    parser.add_argument('--rows', type=int, default=5000000,
                       help='Number of rows to generate (default: 5000000)')
    parser.add_argument('--topic', type=str, required=True,
                       help='Kafka topic name')
    parser.add_argument('--format', type=str, default='delta',
                       choices=['delta', 'hudi', 'iceberg'],
                       help='Target data lake format (default: delta)')
    parser.add_argument('--bootstrap-servers', type=str,
                       default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092 for local development)')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Batch size for progress reporting (default: 10000)')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("KAFKA CSV BATCH PRODUCER")
    print("="*70)
    print(f"Rows to generate: {args.rows:,}")
    print(f"Target format: {args.format}")
    print(f"Kafka topic: {args.topic}")
    print(f"Kafka servers: {args.bootstrap_servers}")
    print("="*70 + "\n")
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session()
    
    try:
        # Generate CSV data
        df = generate_csv_data(spark, args.rows, args.format)
        
        # Show sample data
        print("\nSample generated data:")
        df.show(10, truncate=False)
        print(f"\nSchema:")
        df.printSchema()
        
        # Write to Kafka
        rows_written = write_csv_to_kafka(
            df, 
            args.bootstrap_servers, 
            args.topic, 
            args.batch_size
        )
        
        print("\n" + "="*70)
        print("BATCH PRODUCTION COMPLETE")
        print("="*70)
        print(f"Total rows written to Kafka: {rows_written:,}")
        print(f"Topic: {args.topic}")
        print(f"Format: {args.format}")
        print("="*70)
        print("\nYou can now run the batch CSV consumer scripts to process this data.")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()

