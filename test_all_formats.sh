#!/bin/bash

# Test script for all Kafka batch CSV consumers and performance comparison

echo "========================================="
echo "TESTING ALL KAFKA BATCH CSV CONSUMERS"
echo "========================================="

TOPIC="batch-csv-topic"
BOOTSTRAP_SERVERS="localhost:9092"

# Test 1: Delta Lake
echo ""
echo "1. Testing Delta Lake..."
python kafka_csv_to_delta.py \
  --topic $TOPIC \
  --schema kafka_test_delta \
  --table iot_events \
  --bootstrap-servers $BOOTSTRAP_SERVERS

echo ""
echo "Press Enter to continue to Hudi CoW test..."
read

# Test 2: Hudi Copy-on-Write
echo ""
echo "2. Testing Hudi Copy-on-Write..."
python kafka_csv_to_hudi.py \
  --topic $TOPIC \
  --schema kafka_test_hudi \
  --table iot_events_cow \
  --table-type COPY_ON_WRITE \
  --primary-key uuid \
  --bootstrap-servers $BOOTSTRAP_SERVERS

echo ""
echo "Press Enter to continue to Hudi MoR test..."
read

# Test 3: Hudi Merge-on-Read
echo ""
echo "3. Testing Hudi Merge-on-Read..."
python kafka_csv_to_hudi.py \
  --topic $TOPIC \
  --schema kafka_test_hudi \
  --table iot_events_mor \
  --table-type MERGE_ON_READ \
  --primary-key uuid \
  --bootstrap-servers $BOOTSTRAP_SERVERS

echo ""
echo "Press Enter to continue to Iceberg test..."
read

# Test 4: Iceberg
echo ""
echo "4. Testing Iceberg..."
python kafka_csv_to_iceberg.py \
  --topic $TOPIC \
  --schema kafka_test_iceberg \
  --table iot_events \
  --bootstrap-servers $BOOTSTRAP_SERVERS

echo ""
echo "========================================="
echo "ALL INDIVIDUAL TESTS COMPLETE"
echo "========================================="
echo ""
echo "Now running combined performance comparison..."
echo "Press Enter to continue..."
read

# Combined Performance Comparison
python orchestration.py \
  --topic $TOPIC \
  --schema kafka_perf_test \
  --table iot_events \
  --bootstrap-servers $BOOTSTRAP_SERVERS \
  --compare-hudi-types \
  --output kafka_performance_results.json

echo ""
echo "========================================="
echo "ALL TESTS COMPLETE!"
echo "Results saved to: kafka_performance_results.json"
echo "========================================="

