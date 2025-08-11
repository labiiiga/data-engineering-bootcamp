from pyspark.sql import SparkSession


KEYFILE_PATH = "/opt/spark/pyspark/deb-uploading-to-gcs.json"

spark = SparkSession.builder.appName("demo_gcs") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

GCS_FILE_PATH = "gs://deb_bootcamp_162539/raw/greenery/addresses/addresses.csv"
df = spark.read.option("header", True).csv(GCS_FILE_PATH)
df.show()

df.createOrReplaceTempView("addresses")
result = spark.sql("""
    select
        *

    from addresses
""")

OUTPUT_PATH = "gs://deb_bootcamp_162539/my_spark"
result.write.mode("overwrite").parquet(OUTPUT_PATH)
