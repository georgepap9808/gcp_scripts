from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from google.cloud import storage
import sys

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts")
blob = bucket.blob("spark_config_iceberg.py")
blob.download_to_filename("/tmp/spark_config_iceberg.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import your file as a module
from spark_config_iceberg import create_spark_session

def IngestIcebergCSVHeader(spark, iDBSchema, iTable, iFilePath, collect_metrics=True):
    """
    Ingest CSV file into Apache Iceberg table with performance metrics collection.
    
    Args:
        spark: SparkSession
        iDBSchema: Database schema name
        iTable: Table name
        iFilePath: GCS path to CSV file
        collect_metrics: Whether to collect and print performance metrics
    
    Returns:
        dict: Performance metrics if collect_metrics=True, else None
    """
    import time
    
    metrics = {
        'format': 'iceberg',
        'schema': iDBSchema,
        'table': iTable,
        'source_file': iFilePath,
        'start_time': time.time(),
        'read_time': 0,
        'write_time': 0,
        'total_time': 0,
        'row_count': 0,
        'file_size_mb': 0,
        'success': False
    }
    
    try:
        # Read the CSV file with error handling
        print(f"Reading CSV from: {iFilePath}")
        read_start = time.time()
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(iFilePath)
        
        # Cache to get accurate row count
        df.cache()
        row_count = df.count()
        metrics['row_count'] = row_count
        metrics['read_time'] = time.time() - read_start

        print(f"Preview of data:")
        df.show(5)
        print(f"Schema:")
        df.printSchema()
        print(f"Total rows read: {row_count:,}")

        # Create schema if not exists
        try:
            spark.sql(f"create database if not exists {iDBSchema}")
            print(f"Schema {iDBSchema} created or already exists")
        except Exception as e:
            print(f"Error creating schema {iDBSchema}: {str(e)}")
            raise

        # Get table location path (using osd-data2 for consistency)
        table_path = f"gs://osd-data2/{iDBSchema}.db/{iTable}"

        # Drop existing table if it exists
        spark.sql(f"DROP TABLE IF EXISTS {iDBSchema}.{iTable}")

        # Create Iceberg table with optimized properties
        # Convert Spark types to SQL types
        type_mapping = {
            'StringType': 'string',
            'IntegerType': 'int',
            'LongType': 'bigint',
            'DoubleType': 'double',
            'FloatType': 'float',
            'BooleanType': 'boolean',
            'TimestampType': 'timestamp',
            'DateType': 'date'
        }
        
        columns = []
        for col, dtype in df.dtypes:
            sql_type = type_mapping.get(dtype, 'string')
            columns.append(f"{col} {sql_type}")
        
        create_table_sql = f"""
        CREATE TABLE {iDBSchema}.{iTable} (
            {', '.join(columns)}
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
        print(f"Created Iceberg table: {iDBSchema}.{iTable}")

        # Write data using simple DataFrame API
        print(f"Writing to Iceberg table at: {table_path}")
        write_start = time.time()
        df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"{iDBSchema}.{iTable}")
        
        metrics['write_time'] = time.time() - write_start
        print(f"Successfully written data to {table_path}")

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.table(f"{iDBSchema}.{iTable}")
        verify_count = read_df.count()
        read_df.show(5)
        print(f"Verified row count: {verify_count:,}")
        
        metrics['total_time'] = time.time() - metrics['start_time']
        metrics['success'] = True
        
        if collect_metrics:
            print("\n" + "="*60)
            print("PERFORMANCE METRICS - Apache Iceberg")
            print("="*60)
            print(f"Format: Apache Iceberg")
            print(f"Schema: {iDBSchema}")
            print(f"Table: {iTable}")
            print(f"Source: {iFilePath}")
            print(f"Rows Processed: {metrics['row_count']:,}")
            print(f"Read Time: {metrics['read_time']:.2f} seconds")
            print(f"Write Time: {metrics['write_time']:.2f} seconds")
            print(f"Total Time: {metrics['total_time']:.2f} seconds")
            if metrics['row_count'] > 0:
                print(f"Throughput: {metrics['row_count'] / metrics['total_time']:,.0f} rows/second")
            print("="*60 + "\n")
        
        return metrics if collect_metrics else None

    except Exception as e:
        metrics['total_time'] = time.time() - metrics['start_time']
        metrics['success'] = False
        metrics['error'] = str(e)
        print(f"Error processing {iFilePath}: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        if collect_metrics:
            return metrics
        raise

def main():
    spark = create_spark_session()
    all_metrics = []

    try:
        print("Spark Session created successfully")

        # Show available databases
        print("Available databases:")
        spark.sql("show databases").show()

        # Define tables to ingest
        # Can be overridden via command line arguments
        import argparse
        parser = argparse.ArgumentParser(description='Apache Iceberg CSV Batch Ingestion')
        parser.add_argument('--schema', type=str, default='restaurant_iceberg',
                          help='Database schema name')
        parser.add_argument('--table', type=str, default=None,
                          help='Table name (if not provided, ingests all default tables)')
        parser.add_argument('--file', type=str, default=None,
                          help='GCS path to CSV file')
        parser.add_argument('--source-bucket', type=str, default='osd-data',
                          help='Source GCS bucket for CSV files')
        
        args = parser.parse_args()
        
        if args.table and args.file:
            # Single table ingestion
            tables_to_ingest = [(args.schema, args.table, args.file)]
        else:
            # Default tables
            tables_to_ingest = [
                (args.schema, "menu", f"gs://{args.source_bucket}/source/menu_items.csv"),
                (args.schema, "orders", f"gs://{args.source_bucket}/source/order_details.csv"),
                (args.schema, "db_dictionary", f"gs://{args.source_bucket}/source/data_dictionary.csv")
            ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            print(f"\n{'='*60}")
            print(f"Processing: {schema}.{table}")
            print(f"{'='*60}")
            metrics = IngestIcebergCSVHeader(spark, schema, table, path, collect_metrics=True)
            if metrics:
                all_metrics.append(metrics)

        print("\n" + "="*60)
        print("BATCH INGESTION SUMMARY - Apache Iceberg")
        print("="*60)
        if all_metrics:
            total_rows = sum(m['row_count'] for m in all_metrics)
            total_time = sum(m['total_time'] for m in all_metrics)
            successful = sum(1 for m in all_metrics if m['success'])
            print(f"Tables Processed: {len(all_metrics)}")
            print(f"Successful: {successful}")
            print(f"Total Rows: {total_rows:,}")
            print(f"Total Time: {total_time:.2f} seconds")
            if total_time > 0:
                print(f"Overall Throughput: {total_rows / total_time:,.0f} rows/second")
        print("="*60)
        print("\nAll tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()