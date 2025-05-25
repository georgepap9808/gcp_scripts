from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

SRC  = "gs://osd-scripts2/csv/iot_events_10m.csv"
DEST = "gs://osd-data2/hudi/iot_events"          # ✱ your lake path

df = spark.read.option("header", "true").csv(SRC)
df = df.withColumn("ts", col("ts").cast("timestamp"))       # ensure types

hudi_options = {
    "hoodie.table.name": "iot_events",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.table.name": "iot_events",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.table": "iot_events",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.partition_fields": "date",
    "hoodie.datasource.hive_sync.jdbcurl": "jdbc:postgresql://postgres:5432/hive_metastore",
}

(df.write
   .format("hudi")
   .options(**hudi_options)
   .mode("overwrite")
   .save(DEST))
