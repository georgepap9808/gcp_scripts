"""
Kafka CSV to Apache Iceberg - Batch CSV Consumer

This script reads CSV-formatted messages from Kafka and processes them into Apache Iceberg
in batch mode (processes all messages and stops).

Usage:
    python kafka_csv_to_iceberg.py --topic batch-csv-topic --schema kafka_iceberg_batch --table iot_events
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import sys
import time
import argparse

# Try to use local config file first, fall back to GCS download
config_file = "spark_config_iceberg.py"
local_path = os.path.join(os.path.dirname(__file__), config_file)

if os.path.exists(local_path):
    # Use local config file (for local development)
    sys.path.insert(0, os.path.dirname(local_path))
    print(f"Using local config file: {local_path}")
else:
    # Download from GCS if local file doesn't exist
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.get_bucket("osd-scripts2")
        blob = bucket.blob(config_file)
        blob.download_to_filename("/tmp/" + config_file)
        sys.path.insert(0, '/tmp')
        print(f"Downloaded config file from GCS: {config_file}")
    except Exception as e:
        print(f"Error: Could not find local config file and failed to download from GCS: {e}")
        raise

# Import the config module
from spark_config_iceberg import create_spark_session


def parse_csv_value(df):
    """
    Parse CSV string values from Kafka messages into structured DataFrame.
    
    Expected CSV format: uuid,ts,consumption,month,day,hour,minute,date,key
    
    Args:
        df: DataFrame with Kafka message format (key, value, timestamp)
    
    Returns:
        DataFrame with parsed CSV columns
    """
    # Parse CSV string into columns
    split_cols = F.split(F.col("value"), ",")
    
    parsed_df = df.select(
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        split_cols[0].alias("uuid"),
        F.to_timestamp(split_cols[1]).alias("ts"),
        split_cols[2].cast(DoubleType()).alias("consumption"),
        split_cols[3].alias("month"),
        split_cols[4].alias("day"),
        split_cols[5].alias("hour"),
        split_cols[6].alias("minute"),
        split_cols[7].alias("date"),
        split_cols[8].alias("key")
    ).filter(F.col("uuid").isNotNull())  # Filter out invalid rows
    
    return parsed_df


def process_batch_csv_to_iceberg(spark, db_schema, table_name, topic, bootstrap_servers):
    """
    Process CSV messages from Kafka in batch mode and write to Apache Iceberg.
    
    Args:
        spark: SparkSession
        db_schema: Database schema name
        table_name: Table name
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
    """
    metrics = {
        'format': 'iceberg',
        'schema': db_schema,
        'table': table_name,
        'start_time': time.time(),
        'kafka_read_time': 0,
        'parse_time': 0,
        'cache_time': 0,
        'read_time': 0,  # Total read time (kafka + parse + cache)
        'write_time': 0,
        'verify_time': 0,
        'total_time': 0,
        'row_count': 0,
        'kafka_message_count': 0,
        'success': False
    }
    
    try:
        print(f"\n{'='*70}")
        print(f"BATCH CSV TO APACHE ICEBERG")
        print(f"{'='*70}")
        print(f"Topic: {topic}")
        print(f"Schema: {db_schema}")
        print(f"Table: {table_name}")
        print(f"{'='*70}\n")
        
        # Determine table path based on warehouse dir (local vs GCS)
        warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse/")
        # Ensure warehouse_dir ends with /
        if not warehouse_dir.endswith("/"):
            warehouse_dir += "/"
        if warehouse_dir.startswith("gs://"):
            table_path = f"{warehouse_dir}{db_schema}.db/{table_name}"
            # Create schema if not exists (only needed for Hive metastore)
            try:
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_schema}")
            except:
                pass  # Skip if Hive not available
        else:
            # Local file system - no need for database creation
            table_path = f"{warehouse_dir}{db_schema}.db/{table_name}"
            # Create directory structure (remove file:// prefix for os.makedirs)
            import os
            local_path = table_path.replace("file://", "")
            os.makedirs(local_path, exist_ok=True)
        
        # Read all messages from Kafka (batch mode)
        print(f"Reading CSV messages from Kafka topic: {topic}")
        kafka_read_start = time.time()
        
        df_kafka = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        kafka_count_start = time.time()
        kafka_message_count = df_kafka.count()
        metrics['kafka_message_count'] = kafka_message_count
        metrics['kafka_read_time'] = time.time() - kafka_read_start
        print(f"Raw Kafka messages count: {kafka_message_count:,}")
        print(f"Kafka read time: {metrics['kafka_read_time']:.2f} seconds")
        
        # Parse CSV values
        print("Parsing CSV format...")
        parse_start = time.time()
        parsed_df = parse_csv_value(df_kafka)
        metrics['parse_time'] = time.time() - parse_start
        print(f"CSV parse time: {metrics['parse_time']:.2f} seconds")
        
        # Cache for operations
        print("Caching parsed data...")
        cache_start = time.time()
        parsed_df.cache()
        row_count = parsed_df.count()
        metrics['cache_time'] = time.time() - cache_start
        metrics['row_count'] = row_count
        metrics['read_time'] = metrics['kafka_read_time'] + metrics['parse_time'] + metrics['cache_time']
        
        print(f"Parsed CSV rows: {row_count:,}")
        print(f"Read time: {metrics['read_time']:.2f} seconds")
        
        # Show sample data
        print("\nSample parsed data:")
        parsed_df.show(10, truncate=False)
        print("\nSchema:")
        parsed_df.printSchema()
        
        # Add processing metadata
        final_df = parsed_df.withColumn("processing_time", F.current_timestamp())
        
        # Try to drop existing table if it exists (only if catalog supports it)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {db_schema}.{table_name}")
        except:
            pass  # Skip if table doesn't exist or catalog doesn't support it
        
        # Write to Iceberg - use save directly to path if catalog not available
        print(f"\nWriting to Iceberg table at: {table_path}")
        write_start = time.time()
        
        try:
            # Try using saveAsTable first (requires catalog)
            final_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .saveAsTable(f"{db_schema}.{table_name}")
            print("Written using saveAsTable (catalog)")
        except Exception as e:
            # Fall back to writing directly to path (for local/Hadoop catalog)
            print(f"Note: saveAsTable not available, writing directly to path: {e}")
            final_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("path", table_path) \
                .save()
            print(f"Written directly to: {table_path}")
        
        metrics['write_time'] = time.time() - write_start
        print(f"Write time: {metrics['write_time']:.2f} seconds")
        
        # Verify the write
        print("\nVerifying written data:")
        verify_start = time.time()
        try:
            verify_df = spark.sql(f"SELECT * FROM {db_schema}.{table_name}")
        except:
            # Fall back to reading from path
            verify_df = spark.read.format("iceberg").load(table_path)
        verify_count = verify_df.count()
        metrics['verify_time'] = time.time() - verify_start
        verify_df.show(5)
        print(f"Verified row count: {verify_count:,}")
        print(f"Verification time: {metrics['verify_time']:.2f} seconds")
        
        # Print statistics (simplified to avoid JVM crashes with large datasets)
        print("\nTable statistics:")
        try:
            # Use simpler aggregations to avoid JVM crashes
            total_records = verify_count
            # Get basic stats without complex aggregations
            print(f"Total Records: {total_records:,}")
            print(f"Note: Skipping complex aggregations to avoid JVM crashes with large datasets")
            print(f"Data successfully written and verified: {verify_count:,} rows")
        except Exception as stats_error:
            print(f"Note: Could not compute statistics (JVM issue): {stats_error}")
            print(f"Data successfully written: {verify_count:,} rows")
        
        metrics['total_time'] = time.time() - metrics['start_time']
        metrics['success'] = True
        
        # Print detailed performance metrics
        print("\n" + "="*70)
        print("DETAILED PERFORMANCE METRICS")
        print("="*70)
        print(f"Format: Apache Iceberg")
        print(f"Schema: {db_schema}")
        print(f"Table: {table_name}")
        print(f"Kafka Topic: {topic}")
        print("-"*70)
        print(f"DATA METRICS:")
        print(f"  Kafka Messages: {metrics['kafka_message_count']:,}")
        print(f"  Rows Processed: {metrics['row_count']:,}")
        print(f"  Verified Rows: {verify_count:,}")
        print("-"*70)
        print(f"TIMING BREAKDOWN:")
        print(f"  Kafka Read:     {metrics['kafka_read_time']:>8.2f} seconds ({metrics['kafka_read_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  CSV Parse:       {metrics['parse_time']:>8.2f} seconds ({metrics['parse_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  Cache:           {metrics['cache_time']:>8.2f} seconds ({metrics['cache_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  Total Read:      {metrics['read_time']:>8.2f} seconds ({metrics['read_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  Write:           {metrics['write_time']:>8.2f} seconds ({metrics['write_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  Verify:          {metrics['verify_time']:>8.2f} seconds ({metrics['verify_time']/metrics['total_time']*100:>5.1f}%)")
        print(f"  Total Time:      {metrics['total_time']:>8.2f} seconds (100.0%)")
        print("-"*70)
        print(f"THROUGHPUT METRICS:")
        if metrics['read_time'] > 0:
            read_throughput = metrics['row_count'] / metrics['read_time']
            print(f"  Read Throughput:  {read_throughput:>12,.0f} rows/second")
        if metrics['write_time'] > 0:
            write_throughput = metrics['row_count'] / metrics['write_time']
            print(f"  Write Throughput: {write_throughput:>12,.0f} rows/second")
        if metrics['total_time'] > 0:
            total_throughput = metrics['row_count'] / metrics['total_time']
            print(f"  Total Throughput: {total_throughput:>12,.0f} rows/second")
        print("="*70)
        
        parsed_df.unpersist()
        
        return metrics
        
    except Exception as e:
        metrics['total_time'] = time.time() - metrics['start_time']
        metrics['success'] = False
        metrics['error'] = str(e)
        print(f"\n❌ Error processing batch: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        return metrics


def main():
    parser = argparse.ArgumentParser(
        description='Process CSV messages from Kafka and write to Apache Iceberg',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process CSV from Kafka to Iceberg
  python kafka_csv_to_iceberg.py --topic batch-csv-topic --schema kafka_iceberg_batch --table iot_events
  
  # Custom Kafka servers
  python kafka_csv_to_iceberg.py --topic batch-csv-topic --schema kafka_iceberg_batch --table iot_events --bootstrap-servers localhost:9092
        """
    )
    
    parser.add_argument('--topic', type=str, required=True,
                       help='Kafka topic name')
    parser.add_argument('--schema', type=str, required=True,
                       help='Database schema name')
    parser.add_argument('--table', type=str, required=True,
                       help='Table name')
    parser.add_argument('--bootstrap-servers', type=str,
                       default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092 for local development)')
    
    args = parser.parse_args()
    
    print("Creating Spark session...")
    spark = create_spark_session()
    
    try:
        metrics = process_batch_csv_to_iceberg(
            spark, 
            args.schema, 
            args.table, 
            args.topic, 
            args.bootstrap_servers
        )
        
        if metrics['success']:
            print("\n✅ Batch CSV processing completed successfully!")
            sys.exit(0)
        else:
            print(f"\n❌ Batch CSV processing failed: {metrics.get('error', 'Unknown error')}")
            sys.exit(1)
            
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

