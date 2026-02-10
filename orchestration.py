"""
Orchestration Script for Kafka CSV Batch Ingestion

This script orchestrates batch CSV ingestion from Kafka topic across all three data lake formats
(Delta Lake, Apache Hudi, Apache Iceberg) and compares their performance metrics.

Usage:
    python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events
    python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --primary-key uuid
    python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --compare-hudi-types
"""

import sys
import time
import argparse
import json
from datetime import datetime
import os

# Add current directory to path to import local modules
sys.path.insert(0, os.path.dirname(__file__))


def run_ingestion_for_format(format_name, schema_base, table, topic, bootstrap_servers, 
                            primary_key=None, table_type=None):
    """
    Run batch Kafka CSV ingestion for a specific format and return metrics.
    
    Args:
        format_name: Format name ('delta', 'hudi', 'iceberg')
        schema_base: Base schema name
        table: Table name
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
        primary_key: Primary key for Hudi (optional)
        table_type: Table type for Hudi ('COPY_ON_WRITE' or 'MERGE_ON_READ', optional)
    
    Returns:
        dict: Performance metrics or None if failed
    """
    print(f"\n{'='*70}")
    print(f"RUNNING INGESTION: {format_name.upper()}")
    if table_type:
        print(f"Table Type: {table_type}")
    print(f"{'='*70}")
    
    try:
        # Import appropriate spark config
        if format_name == 'delta':
            from spark_config_delta import create_spark_session
            from kafka_csv_to_delta import process_batch_csv_to_delta
        elif format_name == 'hudi':
            from spark_config_hudi import create_spark_session
            from kafka_csv_to_hudi import process_batch_csv_to_hudi
        else:  # iceberg
            from spark_config_iceberg import create_spark_session
            from kafka_csv_to_iceberg import process_batch_csv_to_iceberg
        
        # Create Spark session
        spark = create_spark_session()
        
        try:
            # Create schema name with format suffix
            if format_name == 'hudi' and table_type:
                table_type_suffix = f"_{table_type.lower()}"
                schema = f"{schema_base}_hudi{table_type_suffix}"
                table_name = f"{table}_{table_type.lower()}"
            else:
                schema = f"{schema_base}_{format_name}"
                table_name = table
            
            # Run ingestion
            if format_name == 'delta':
                metrics = process_batch_csv_to_delta(
                    spark, schema, table_name, topic, bootstrap_servers
                )
            elif format_name == 'hudi':
                hudi_table_type = table_type if table_type else 'COPY_ON_WRITE'
                metrics = process_batch_csv_to_hudi(
                    spark, schema, table_name, topic, bootstrap_servers,
                    hudi_table_type, primary_key or 'uuid'
                )
                metrics['table_type'] = hudi_table_type
            else:  # iceberg
                metrics = process_batch_csv_to_iceberg(
                    spark, schema, table_name, topic, bootstrap_servers
                )
            
            return metrics
            
        finally:
            spark.stop()
            
    except Exception as e:
        print(f"\n❌ Error running {format_name} ingestion: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        return {
            'format': format_name,
            'success': False,
            'error': str(e),
            'schema': schema_base,
            'table': table,
            'row_count': 0,
            'read_time': 0,
            'write_time': 0,
            'total_time': 0
        }


def print_comparison_table(all_metrics):
    """Print a formatted comparison table of all metrics"""
    
    print("\n" + "="*100)
    print("PERFORMANCE COMPARISON RESULTS")
    print("="*100)
    
    # Table header
    header = f"{'Format':<20} {'Status':<12} {'Rows':>15} {'Read (s)':>12} {'Write (s)':>12} {'Total (s)':>12} {'Throughput':>15}"
    print(header)
    print("-" * 100)
    
    # Sort by total time (fastest first)
    sorted_metrics = sorted([m for m in all_metrics if m], 
                          key=lambda x: x.get('total_time', float('inf')))
    
    for metrics in sorted_metrics:
        if not metrics:
            continue
            
        format_name = metrics.get('format', 'unknown').upper()
        if format_name == 'HUDI' and metrics.get('table_type'):
            format_name = f"HUDI ({metrics.get('table_type')})"
        
        status = "✅ Success" if metrics.get('success') else "❌ Failed"
        row_count = metrics.get('row_count', 0)
        read_time = metrics.get('read_time', 0)
        write_time = metrics.get('write_time', 0)
        total_time = metrics.get('total_time', 0)
        
        throughput = (row_count / total_time) if total_time > 0 else 0
        
        row = f"{format_name:<20} {status:<12} {row_count:>15,} {read_time:>12.2f} {write_time:>12.2f} {total_time:>12.2f} {throughput:>15,.0f}"
        print(row)
    
    print("="*100)
    
    # Identify winners
    successful_metrics = [m for m in sorted_metrics if m and m.get('success')]
    
    if successful_metrics:
        fastest = min(successful_metrics, key=lambda x: x.get('total_time', float('inf')))
        fastest_write = min(successful_metrics, key=lambda x: x.get('write_time', float('inf')))
        highest_throughput = max(successful_metrics, 
                               key=lambda x: (x.get('row_count', 0) / x.get('total_time', 1)) if x.get('total_time', 0) > 0 else 0)
        
        print("\n🏆 WINNERS:")
        fastest_name = fastest.get('format', 'unknown').upper()
        if fastest_name == 'HUDI' and fastest.get('table_type'):
            fastest_name = f"HUDI ({fastest.get('table_type')})"
        print(f"  Fastest Overall: {fastest_name} ({fastest.get('total_time', 0):.2f}s)")
        
        fastest_write_name = fastest_write.get('format', 'unknown').upper()
        if fastest_write_name == 'HUDI' and fastest_write.get('table_type'):
            fastest_write_name = f"HUDI ({fastest_write.get('table_type')})"
        print(f"  Fastest Write: {fastest_write_name} ({fastest_write.get('write_time', 0):.2f}s)")
        
        highest_throughput_name = highest_throughput.get('format', 'unknown').upper()
        if highest_throughput_name == 'HUDI' and highest_throughput.get('table_type'):
            highest_throughput_name = f"HUDI ({highest_throughput.get('table_type')})"
        throughput_val = (highest_throughput.get('row_count', 0) / highest_throughput.get('total_time', 1)) if highest_throughput.get('total_time', 0) > 0 else 0
        print(f"  Highest Throughput: {highest_throughput_name} ({throughput_val:,.0f} rows/s)")




def main():
    parser = argparse.ArgumentParser(
        description='Orchestrate batch CSV ingestion performance comparison from Kafka across Delta, Hudi, and Iceberg',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare all formats
  python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --bootstrap-servers localhost:9092
  
  # Compare with custom primary key for Hudi
  python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --primary-key uuid --bootstrap-servers localhost:9092
  
  # Compare including both Hudi CoW and MoR
  python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --compare-hudi-types --bootstrap-servers localhost:9092
  
  # Compare specific formats only
  python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --formats delta hudi --bootstrap-servers localhost:9092
  
  # Save results to JSON
  python orchestration.py --topic batch-csv-topic --schema kafka_batch --table iot_events --output results.json --bootstrap-servers localhost:9092
        """
    )
    
    parser.add_argument('--topic', type=str, required=True,
                       help='Kafka topic name')
    parser.add_argument('--schema', type=str, required=True,
                       help='Base schema name (will append _delta, _hudi, _iceberg)')
    parser.add_argument('--table', type=str, required=True,
                       help='Table name')
    parser.add_argument('--primary-key', type=str, default='uuid',
                       help='Primary key column (for Hudi, default: uuid)')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
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
    print("KAFKA BATCH CSV INGESTION ORCHESTRATION")
    print("="*100)
    print(f"Kafka Topic: {args.topic}")
    print(f"Bootstrap Servers: {args.bootstrap_servers}")
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
                                                      args.topic, args.bootstrap_servers, 
                                                      args.primary_key, 'COPY_ON_WRITE')
                all_metrics.append(metrics_cow)
                
                # Run MoR
                metrics_mor = run_ingestion_for_format(format_name, args.schema, args.table, 
                                                      args.topic, args.bootstrap_servers, 
                                                      args.primary_key, 'MERGE_ON_READ')
                all_metrics.append(metrics_mor)
            else:
                # Default Hudi type is CoW
                table_type = None
                if format_name == 'hudi':
                    table_type = 'COPY_ON_WRITE'
                
                metrics = run_ingestion_for_format(format_name, args.schema, args.table, 
                                                  args.topic, args.bootstrap_servers, 
                                                  args.primary_key, table_type)
                all_metrics.append(metrics)
                
        except Exception as e:
            print(f"\n❌ Failed to process {format_name}: {str(e)}")
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
        output_data = {
            'comparison_timestamp': datetime.now().isoformat(),
            'topic': args.topic,
            'schema': args.schema,
            'table': args.table,
            'primary_key': args.primary_key,
            'total_comparison_time_seconds': total_comparison_time,
            'metrics': all_metrics
        }
        
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n✅ Results saved to {args.output}")
    
    print(f"\n⏱️  Total orchestration time: {total_comparison_time:.2f} seconds")
    print("="*100)


if __name__ == "__main__":
    sys.exit(main())

