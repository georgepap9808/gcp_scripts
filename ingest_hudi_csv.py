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
blob = bucket.blob("spark_config_hudi.py")
blob.download_to_filename("/tmp/spark_config_hudi.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import your file as a module
from spark_config_hudi import create_spark_session

def IngestHudiCSVHeader(spark, iDBSchema, iTable, iFilePath, collect_metrics=True, primary_key=None, table_type='COPY_ON_WRITE'):
    """
    Ingest CSV file into Apache Hudi table with performance metrics collection.
    
    Args:
        spark: SparkSession
        iDBSchema: Database schema name
        iTable: Table name
        iFilePath: GCS path to CSV file
        collect_metrics: Whether to collect and print performance metrics
        primary_key: Primary key column name (auto-detected if None)
        table_type: Hudi table type ('COPY_ON_WRITE' or 'MERGE_ON_READ', default: 'COPY_ON_WRITE')
    
    Returns:
        dict: Performance metrics if collect_metrics=True, else None
    """
    import time
    
    metrics = {
        'format': 'hudi',
        'table_type': table_type,
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

        # Get primary key column
        if primary_key:
            pk_column = primary_key
        else:
            # Try common primary key names, otherwise use first column
            pk_candidates = ['id', 'uuid', 'key', 'record_id', 'primary_key']
            pk_column = None
            for candidate in pk_candidates:
                if candidate in df.columns:
                    pk_column = candidate
                    break
            if not pk_column:
                pk_column = df.columns[0]
        
        print(f"Using {pk_column} as the primary key")
        print(f"Using table type: {table_type}")

        # Hudi write options
        hudiOptions = {
            'hoodie.table.name': f"{iDBSchema}_{iTable}",
            'hoodie.datasource.write.recordkey.field': pk_column,
            'hoodie.datasource.write.precombine.field': pk_column,
            'hoodie.datasource.write.operation': 'bulk_insert',
            'hoodie.bulkinsert.shuffle.parallelism': '2',
            'hoodie.datasource.write.table.type': table_type,
            'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
            'hoodie.cleaner.commits.retained': '10',
            'hoodie.keep.min.commits': '20',
            'hoodie.keep.max.commits': '30'
        }
        
        # Add MoR-specific options
        if table_type == 'MERGE_ON_READ':
            hudiOptions.update({
                'hoodie.compact.inline': 'true',
                'hoodie.compact.inline.max.delta.commits': '5',
                'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
            })

        # Write to Hudi table
        print(f"Writing to Hudi table at: {table_path}")
        write_start = time.time()
        df.write \
            .format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("overwrite") \
            .save(table_path)
        
        metrics['write_time'] = time.time() - write_start
        print(f"Successfully written data to {table_path}")

        # Register table in Hive metastore
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
            USING hudi
            LOCATION '{table_path}'
        """)

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.read.format("hudi").load(table_path)
        verify_count = read_df.count()
        read_df.show(5)
        print(f"Verified row count: {verify_count:,}")
        
        metrics['total_time'] = time.time() - metrics['start_time']
        metrics['success'] = True
        
        if collect_metrics:
            print("\n" + "="*60)
            print("PERFORMANCE METRICS - Apache Hudi")
            print("="*60)
            print(f"Format: Apache Hudi ({table_type})")
            print(f"Schema: {iDBSchema}")
            print(f"Table: {iTable}")
            print(f"Source: {iFilePath}")
            print(f"Primary Key: {pk_column}")
            print(f"Table Type: {table_type}")
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
        parser = argparse.ArgumentParser(description='Apache Hudi CSV Batch Ingestion')
        parser.add_argument('--schema', type=str, default='restaurant_hudi',
                          help='Database schema name')
        parser.add_argument('--table', type=str, default=None,
                          help='Table name (if not provided, ingests all default tables)')
        parser.add_argument('--file', type=str, default=None,
                          help='GCS path to CSV file')
        parser.add_argument('--source-bucket', type=str, default='osd-data',
                          help='Source GCS bucket for CSV files')
        parser.add_argument('--primary-key', type=str, default=None,
                          help='Primary key column name (auto-detected if not provided)')
        parser.add_argument('--table-type', type=str, default='COPY_ON_WRITE',
                          choices=['COPY_ON_WRITE', 'MERGE_ON_READ'],
                          help='Hudi table type: COPY_ON_WRITE or MERGE_ON_READ (default: COPY_ON_WRITE)')
        
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
            metrics = IngestHudiCSVHeader(spark, schema, table, path, 
                                        collect_metrics=True, 
                                        primary_key=args.primary_key,
                                        table_type=args.table_type)
            if metrics:
                all_metrics.append(metrics)

        print("\n" + "="*60)
        print("BATCH INGESTION SUMMARY - Apache Hudi")
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
