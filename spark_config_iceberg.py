from pyspark.sql import SparkSession
import os

def create_spark_session():
    # Kafka connector and Iceberg packages (Scala 2.12)
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    iceberg_package = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
    packages = f"{iceberg_package},{kafka_package}"
    
    # Use local warehouse path for local development, GCS for production
    has_gcs_creds = os.path.exists("/mnt/secrets/key.json") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    warehouse_dir = "gs://osd-data2/" if has_gcs_creds else "file:///tmp/spark-warehouse/"
    
    builder = SparkSession.builder \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.warehouse.dir", warehouse_dir)
    
    # Only add Hive and GCS configs if service account key exists (production environment)
    if has_gcs_creds:
        builder = builder \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hive") \
            .config("hive.metastore.warehouse.dir", warehouse_dir) \
            .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
            .config("javax.jdo.option.ConnectionUserName", "hive") \
            .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
            .config("datanucleus.schema.autoCreateTables", "true") \
            .config("hive.metastore.schema.verification", "false") \
            .config("iceberg.engine.hive.lock-enabled", "false") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json") \
            .config("spark.hadoop.fs.gs.project.id", "milan-data-platform-project2") \
            .config("spark.hadoop.fs.gs.system.bucket", "osd-data2")
        
        builder = builder.enableHiveSupport()
    else:
        # For local development, use Hadoop catalog instead of Hive
        builder = builder \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", warehouse_dir)

    return builder.getOrCreate()

