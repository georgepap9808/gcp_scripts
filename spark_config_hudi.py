from pyspark.sql import SparkSession
import os

def create_spark_session():
    # Kafka connector and Hudi packages (Scala 2.12)
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    # Hudi version: using 0.15.0 (0.15.1 not available) or try 1.0.2 for latest
    hudi_package = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
    packages = f"{hudi_package},{kafka_package}"
    
    # Use local warehouse path for local development, GCS for production
    has_gcs_creds = os.path.exists("/mnt/secrets/key.json") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    warehouse_dir = "gs://osd-data2/" if has_gcs_creds else "file:///tmp/spark-warehouse/"
    
    builder = SparkSession.builder \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
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

    return builder.getOrCreate()
