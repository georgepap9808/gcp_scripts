from pyspark.sql import SparkSession

def create_spark_session():
    builder = SparkSession.builder \
        .config("spark.jars.packages", 'org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.1') \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.sql.warehouse.dir", "gs://osd-data2/") \
        .config("hive.metastore.warehouse.dir", "gs://osd-data2/") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "hive") \
        .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
        .config("datanucleus.schema.autoCreateTables", "true") \
        .config("hive.metastore.schema.verification", "false") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json") \
        .config("spark.hadoop.fs.gs.project.id", "milan-data-platform-project2") \
        .config("spark.hadoop.fs.gs.system.bucket", "osd-data2") \
        .enableHiveSupport()

    return builder.getOrCreate()
