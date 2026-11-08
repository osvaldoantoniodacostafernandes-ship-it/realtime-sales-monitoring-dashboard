
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("TestKafkaSpark")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. SCHEMA JSON TRANSAKSI
# ============================================================

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", LongType(), True),
    StructField("total_amount", LongType(), True),
    StructField("payment_method", StringType(), True)
])


# ============================================================
# 3. BACA DATA DARI KAFKA
# ============================================================

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales_transactions")
    .option("startingOffsets", "earliest")
    .load()
)


# ============================================================
# 4. PARSE VALUE JSON
# ============================================================

parsed_df = (
    df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(col("json_value"), schema).alias("data")
    )
    .select("data.*")
)


# ============================================================
# 5. TAMPILKAN HASIL KE CONSOLE
# ============================================================

query = (
    parsed_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "checkpoint/test_kafka_parsed")
    .start()
)


print("========================================")
print("SPARK -> KAFKA JSON PARSING BERHASIL")
print("========================================")


query.awaitTermination()
