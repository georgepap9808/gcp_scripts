from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def create_spark_session():
    # Kafka connector for Spark 3.5.0 (Scala 2.12)
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    
    # Use local warehouse path for local development, GCS for production
    has_gcs_creds = os.path.exists("/mnt/secrets/key.json") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    warehouse_dir = "gs://osd-data2/" if has_gcs_creds else "file:///tmp/spark-warehouse/"
    
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir)
    
    # Only add Hive and GCS configs if service account key exists (production environment)
    if has_gcs_creds:
        builder = builder \
            .config("hive.metastore.warehouse.dir", warehouse_dir) \
            .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
            .config("javax.jdo.option.ConnectionUserName", "hive") \
            .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
            .config("datanucleus.schema.autoCreateTables", "true") \
            .config("hive.metastore.schema.verification", "false")
        
        # Only add GCS configs if service account key exists (for local dev, skip if not available)
        builder = builder \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json") \
            .config("spark.hadoop.fs.gs.project.id", "milan-data-platform-project2") \
            .config("spark.hadoop.fs.gs.system.bucket", "osd-data2")
    
    # Only enable Hive support if GCS credentials are available (production environment)
    # For local development, skip Hive to avoid PostgreSQL dependency
    if has_gcs_creds:
        builder = builder.enableHiveSupport()
    else:
        # Use in-memory metastore for local development
        builder = builder.config("spark.sql.catalogImplementation", "in-memory")

    # Configure Delta with Spark
    # Note: configure_spark_with_delta_pip returns a new builder, so we need to preserve Kafka package config
    delta_builder = configure_spark_with_delta_pip(builder)
    
    # Re-apply Kafka package config after configure_spark_with_delta_pip to ensure it's preserved
    delta_builder = delta_builder.config("spark.jars.packages", kafka_package)
    
    spark = delta_builder.getOrCreate()

    return spark
