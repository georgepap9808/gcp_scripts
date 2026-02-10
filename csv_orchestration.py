"""
CSV Orchestration Script for Batch CSV Ingestion

This script orchestrates batch CSV ingestion from GCS files across all three data lake formats
(Delta Lake, Apache Hudi, Apache Iceberg) and compares their performance metrics.

Usage:
    python csv_orchestration.py --schema restaurant --table menu --file gs://osd-data/source/menu_items.csv
    python csv_orchestration.py --schema restaurant --table orders --file gs://osd-data/source/order_details.csv --primary-key order_id
"""

from pyspark.sql import SparkSession
import sys
import time
import argparse
import json
from datetime import datetime
from google.cloud import storage


def download_spark_config(format_name, bucket_name="osd-scripts2"):
    """Download spark config from GCS"""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    config_file = f"spark_config_{format_name}.py"
    blob = bucket.blob(config_file)
    blob.download_to_filename(f"/tmp/{config_file}")
    sys.path.insert(0, '/tmp')
    return config_file


def run_ingestion_for_format(format_name, schema_base, table, file_path, primary_key=None, table_type=None):
    """
    Run batch ingestion for a specific format and return metrics.
    
    Args:
        format_name: Format name ('delta', 'hudi', 'iceberg')
        schema_base: Base schema name
        table: Table name
        file_path: CSV file path
        primary_key: Primary key for Hudi (optional)
        table_type: Table type for Hudi ('COPY_ON_WRITE' or 'MERGE_ON_READ', optional)
    
    Returns:
        dict: Performance metrics or None if failed
    """
    print(f"\n{'='*70}")
    print(f"RUNNING INGESTION: {format_name.upper()}")
    print(f"{'='*70}")
    
    # Download config
    download_spark_config(format_name)
    
    # Import appropriate modules
    if format_name == 'delta':
        from spark_config_delta import create_spark_session
        from ingest_delta_csv import IngestDeltaCSVHeader
        schema = f"{schema_base}_delta"
        spark = create_spark_session()
        try:
            metrics = IngestDeltaCSVHeader(spark, schema, table, file_path, collect_metrics=True)
            return metrics
        finally:
            spark.stop()
            
    elif format_name == 'hudi':
        from spark_config_hudi import create_spark_session
        from ingest_hudi_csv import IngestHudiCSVHeader
        # Add table type suffix to schema if specified
        table_type_suffix = f"_{table_type.lower()}" if table_type else ""
        schema = f"{schema_base}_hudi{table_type_suffix}"
        spark = create_spark_session()
        try:
            hudi_table_type = table_type if table_type else 'COPY_ON_WRITE'
            metrics = IngestHudiCSVHeader(spark, schema, table, file_path, 
                                        collect_metrics=True, primary_key=primary_key,
                                        table_type=hudi_table_type)
            return metrics
        finally:
            spark.stop()
            
    else:  # iceberg
        from spark_config_iceberg import create_spark_session
        from ingest_iceberg_csv import IngestIcebergCSVHeader
        schema = f"{schema_base}_iceberg"
        spark = create_spark_session()
        try:
            metrics = IngestIcebergCSVHeader(spark, schema, table, file_path, collect_metrics=True)
            return metrics
        finally:
            spark.stop()


def format_time(seconds):
    """Format time in human-readable format"""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        return f"{seconds/60:.2f}m"
    else:
        return f"{seconds/3600:.2f}h"


def print_comparison_table(all_metrics):
    """Print a formatted comparison table of all metrics"""
    print("\n" + "="*100)
    print("PERFORMANCE COMPARISON SUMMARY")
    print("="*100)
    
    # Header
    header = f"{'Format':<20} {'Status':<10} {'Rows':<15} {'Read Time':<12} {'Write Time':<12} {'Total Time':<12} {'Throughput':<15}"
    print(header)
    print("-" * 100)
    
    # Data rows
    for metrics in all_metrics:
        if metrics and metrics.get('success'):
            format_name = metrics.get('format', 'unknown').upper()
            # Add table type info for Hudi
            if format_name == 'HUDI' and metrics.get('table_type'):
                format_name = f"HUDI ({metrics.get('table_type')})"
            status = "✅ Success"
            rows = f"{metrics.get('row_count', 0):,}"
            read_time = format_time(metrics.get('read_time', 0))
            write_time = format_time(metrics.get('write_time', 0))
            total_time = format_time(metrics.get('total_time', 0))
            throughput = f"{metrics.get('row_count', 0) / max(metrics.get('total_time', 1), 0.001):,.0f} rows/s"
            
            row = f"{format_name:<20} {status:<10} {rows:<15} {read_time:<12} {write_time:<12} {total_time:<12} {throughput:<15}"
            print(row)
        else:
            format_name = metrics.get('format', 'unknown').upper() if metrics else 'UNKNOWN'
            if format_name == 'HUDI' and metrics and metrics.get('table_type'):
                format_name = f"HUDI ({metrics.get('table_type')})"
            status = "❌ Failed"
            error = metrics.get('error', 'Unknown error') if metrics else 'No metrics'
            row = f"{format_name:<20} {status:<10} {'N/A':<15} {'N/A':<12} {'N/A':<12} {'N/A':<12} {'N/A':<15}"
            print(row)
            print(f"  Error: {error}")
    
    print("="*100)
    
    # Winner analysis
    successful_metrics = [m for m in all_metrics if m and m.get('success')]
    if len(successful_metrics) > 1:
        print("\n" + "="*100)
        print("WINNER ANALYSIS")
        print("="*100)
        
        # Fastest total time
        fastest = min(successful_metrics, key=lambda x: x.get('total_time', float('inf')))
        print(f"🏆 Fastest Overall: {fastest.get('format', 'unknown').upper()} ({format_time(fastest.get('total_time', 0))})")
        
        # Fastest write time
        fastest_write = min(successful_metrics, key=lambda x: x.get('write_time', float('inf')))
        print(f"⚡ Fastest Write: {fastest_write.get('format', 'unknown').upper()} ({format_time(fastest_write.get('write_time', 0))})")
        
        # Highest throughput
        highest_throughput = max(successful_metrics, 
                               key=lambda x: x.get('row_count', 0) / max(x.get('total_time', 1), 0.001))
        throughput_val = highest_throughput.get('row_count', 0) / max(highest_throughput.get('total_time', 1), 0.001)
        print(f"🚀 Highest Throughput: {highest_throughput.get('format', 'unknown').upper()} ({throughput_val:,.0f} rows/s)")
        
        print("="*100)


def save_metrics_to_json(all_metrics, output_file):
    """Save metrics to JSON file"""
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'comparison_results': all_metrics
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)
    
    print(f"\nMetrics saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Orchestrate batch CSV ingestion performance comparison across Delta Lake, Hudi, and Iceberg',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare all formats for a single table
  python csv_orchestration.py --schema restaurant --table menu --file gs://osd-data/source/menu_items.csv
  
  # Compare with custom primary key for Hudi
  python csv_orchestration.py --schema restaurant --table orders --file gs://osd-data/source/order_details.csv --primary-key order_id
  
  # Compare including Hudi with both CoW and MoR
  python csv_orchestration.py --schema restaurant --table menu --file gs://osd-data/source/menu_items.csv --compare-hudi-types
  
  # Save results to JSON
  python csv_orchestration.py --schema restaurant --table menu --file gs://osd-data/source/menu_items.csv --output results.json
        """
    )
    
    parser.add_argument('--schema', type=str, required=True,
                       help='Base schema name (will append _delta, _hudi, _iceberg)')
    parser.add_argument('--table', type=str, required=True,
                       help='Table name')
    parser.add_argument('--file', type=str, required=True,
                       help='GCS path to CSV file')
    parser.add_argument('--primary-key', type=str, default=None,
                       help='Primary key column (for Hudi, auto-detected if not provided)')
    parser.add_argument('--config-bucket', type=str, default='osd-scripts2',
                       help='GCS bucket containing spark config files')
    parser.add_argument('--output', type=str, default=None,
                       help='Output JSON file to save metrics (optional)')
    parser.add_argument('--formats', type=str, nargs='+', 
                       choices=['delta', 'hudi', 'iceberg'],
                       default=['delta', 'hudi', 'iceberg'],
                       help='Formats to compare (default: all)')
    parser.add_argument('--compare-hudi-types', action='store_true',
                       help='Compare both Hudi CoW and MoR table types (adds extra Hudi run)')
    
    args = parser.parse_args()
    
    print("\n" + "="*100)
    print("CSV BATCH INGESTION ORCHESTRATION")
    print("="*100)
    print(f"Source File: {args.file}")
    print(f"Table: {args.table}")
    print(f"Base Schema: {args.schema}")
    print(f"Formats to Compare: {', '.join(args.formats)}")
    if args.compare_hudi_types:
        print("Hudi Types: Both CoW and MoR")
    if args.primary_key:
        print(f"Primary Key: {args.primary_key}")
    print("="*100)
    
    all_metrics = []
    start_time = time.time()
    
    # Run ingestion for each format
    for format_name in args.formats:
        try:
            # For Hudi, check if we need to compare both types
            if format_name == 'hudi' and args.compare_hudi_types:
                # Run CoW
                metrics_cow = run_ingestion_for_format(format_name, args.schema, args.table, 
                                                      args.file, args.primary_key, 'COPY_ON_WRITE')
                all_metrics.append(metrics_cow)
                
                # Run MoR
                metrics_mor = run_ingestion_for_format(format_name, args.schema, args.table, 
                                                      args.file, args.primary_key, 'MERGE_ON_READ')
                all_metrics.append(metrics_mor)
            else:
                # Default behavior
                table_type = None
                if format_name == 'hudi' and not args.compare_hudi_types:
                    table_type = 'COPY_ON_WRITE'  # Default to CoW
                
                metrics = run_ingestion_for_format(format_name, args.schema, args.table, 
                                                 args.file, args.primary_key, table_type)
                all_metrics.append(metrics)
        except Exception as e:
            print(f"\n❌ Error running {format_name} ingestion: {str(e)}")
            import traceback
            print(traceback.format_exc())
            all_metrics.append({
                'format': format_name,
                'success': False,
                'error': str(e)
            })
    
    total_comparison_time = time.time() - start_time
    
    # Print comparison table
    print_comparison_table(all_metrics)
    
    # Save to JSON if requested
    if args.output:
        save_metrics_to_json(all_metrics, args.output)
    
    print(f"\nTotal orchestration time: {format_time(total_comparison_time)}")
    print("\nOrchestration complete!")
    
    # Return exit code based on success
    successful = sum(1 for m in all_metrics if m and m.get('success'))
    if successful == len(args.formats):
        return 0
    elif successful > 0:
        return 1  # Partial success
    else:
        return 2  # All failed


if __name__ == "__main__":
    sys.exit(main())

