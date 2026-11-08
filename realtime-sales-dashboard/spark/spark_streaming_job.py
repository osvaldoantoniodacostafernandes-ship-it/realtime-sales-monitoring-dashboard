import sys
from pathlib import Path

# =========================================================
# PROJECT ROOT
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import config terlebih dahulu agar HADOOP_HOME dan _JAVA_OPTIONS terpasang
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MYSQL_JDBC_URL,
    MYSQL_JDBC_PROPERTIES,
)

# =========================================================
# PYSPARK IMPORT
# =========================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    window,
    sum as spark_sum,
    count as spark_count,
    to_timestamp,
    when,
    hour,
    round as spark_round,
    broadcast,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)




# =========================================================
# 1. SPARK SESSION (WINDOWS & MEMORY OPTIMIZED)
# =========================================================

spark = (
    SparkSession.builder
    .appName("RealtimeSalesDashboard")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.LocalFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 60)
print("REALTIME SALES DASHBOARD")
print("Spark Streaming -> Kafka -> MySQL")
print("Input Format    -> Semi-Structured JSON")
print("Output Format   -> Structured Data")
print("=" * 60)


# =========================================================
# 2. SCHEMA SEMI-STRUCTURED JSON
# =========================================================

# Struktur JSON dari Python Producer:
#
# {
#   "transaction_id": "...",
#   "transaction_date": "...",
#   "store": {
#       "name": "...",
#       "location": "..."
#   },
#   "product": {
#       "name": "...",
#       "category": "...",
#       "quantity": 1,
#       "price": 10.0
#   },
#   "payment": {
#       "method": "Cash"
#   },
#   "total_amount": 10.0
# }


store_schema = StructType([
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
])


product_schema = StructType([
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])


payment_schema = StructType([
    StructField("method", StringType(), True),
])


transaction_schema = StructType([
    StructField(
        "transaction_id",
        StringType(),
        True
    ),

    StructField(
        "transaction_date",
        StringType(),
        True
    ),

    StructField(
        "store",
        store_schema,
        True
    ),

    StructField(
        "product",
        product_schema,
        True
    ),

    StructField(
        "payment",
        payment_schema,
        True
    ),

    StructField(
        "total_amount",
        DoubleType(),
        True
    ),
])


# =========================================================
# 3. BACA DATA DARI KAFKA
# =========================================================

raw_stream = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )

    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    .option(
        "startingOffsets",
        "latest"
    )

    .load()
)


# =========================================================
# 4. PARSING SEMI-STRUCTURED JSON
# =========================================================

parsed_stream = (
    raw_stream

    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )

    .select(
        from_json(
            col("json_value"),
            transaction_schema
        ).alias("data")
    )

    # -----------------------------------------------------
    # Ubah nested JSON menjadi structured columns
    # -----------------------------------------------------

    .select(
        col("data.transaction_id").alias(
            "transaction_id"
        ),

        col("data.transaction_date").alias(
            "transaction_date"
        ),

        col("data.store.name").alias(
            "store_name"
        ),

        col("data.store.location").alias(
            "store_location"
        ),

        col("data.product.name").alias(
            "product_name"
        ),

        col("data.product.category").alias(
            "category"
        ),

        col("data.product.quantity").alias(
            "quantity"
        ),

        col("data.product.price").alias(
            "price"
        ),

        col("data.payment.method").alias(
            "payment_method"
        ),

        col("data.total_amount").alias(
            "total_amount"
        ),
    )

    # -----------------------------------------------------
    # Convert transaction date menjadi timestamp
    # -----------------------------------------------------

    .withColumn(
        "transaction_ts",
        to_timestamp(
            col("transaction_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # -----------------------------------------------------
    # Data validation
    # -----------------------------------------------------

    .filter(
        col("transaction_id").isNotNull()
    )

    .filter(
        col("transaction_ts").isNotNull()
    )
)


# =========================================================
# 5. SPARK JOIN & FEATURE ENGINEERING -> SIMPAN KE MYSQL
# =========================================================

# Baca metadata catalog untuk operasi JOIN (Bab 5.4 Pedoman)
try:
    catalog_df = (
        spark.read
        .jdbc(
            url=MYSQL_JDBC_URL,
            table="product_catalog",
            properties=MYSQL_JDBC_PROPERTIES,
        )
        .select(
            col("product_name").alias("cat_pname"),
            col("supplier"),
            col("margin_rate"),
            col("stock_tier"),
        )
        .cache()
    )
    print(f"[SPARK JOIN] Product catalog berhasil dimuat untuk join stream ({catalog_df.count()} produk).")
except Exception as err:
    print(f"[WARN] Gagal memuat catalog untuk JOIN: {err}")
    catalog_df = None


def write_transactions_to_mysql(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    # 1. SPARK JOIN DENGAN METADATA KATALOG (Bab 5.4)
    if catalog_df is not None:
        joined_df = batch_df.join(
            broadcast(catalog_df),
            batch_df.product_name == catalog_df.cat_pname,
            how="left"
        ).drop("cat_pname")
    else:
        joined_df = (
            batch_df
            .withColumn("supplier", col("store_name"))
            .withColumn("margin_rate", col("price") * 0.0 + 0.25)
        )

    # 2. SPARK FEATURE ENGINEERING (Bab 5.4)
    # - Supplier fallback
    # - Margin rate fallback
    # - Estimated Profit = total_amount * margin_rate
    # - is_high_value = flag transaksi di atas $35
    # - is_peak_hour = flag transaksi pada jam sibuk belanja (11-13 & 17-19)
    # - bulk_tier = segmentasi kuantitas belanja (Single, Medium, Bulk)
    enriched_df = (
        joined_df
        .withColumn(
            "supplier",
            when(col("supplier").isNull(), "General Dili Trading").otherwise(col("supplier"))
        )
        .withColumn(
            "margin_rate",
            when(col("margin_rate").isNull(), 0.25).otherwise(col("margin_rate"))
        )
        .withColumn(
            "estimated_profit",
            spark_round(col("total_amount") * col("margin_rate"), 2)
        )
        .withColumn(
            "is_high_value",
            when(col("total_amount") >= 35.0, 1).otherwise(0)
        )
        .withColumn(
            "is_peak_hour",
            when(hour(col("transaction_ts")).isin([11, 12, 13, 17, 18, 19]), 1).otherwise(0)
        )
        .withColumn(
            "bulk_tier",
            when(col("quantity") >= 4, "Bulk")
            .when(col("quantity") >= 2, "Medium")
            .otherwise("Single")
        )
    )

    output_df = (
        enriched_df
        .select(
            col("transaction_id"),
            col("store_name"),
            col("store_location"),
            col("transaction_ts").alias("transaction_date"),
            col("product_name"),
            col("category"),
            col("quantity"),
            col("price"),
            col("total_amount"),
            col("payment_method"),
            col("supplier"),
            col("margin_rate"),
            col("estimated_profit"),
            col("is_high_value"),
            col("is_peak_hour"),
            col("bulk_tier"),
        )
    )

    row_count = output_df.count()

    (
        output_df.write
        .jdbc(
            url=MYSQL_JDBC_URL,
            table="transactions",
            mode="append",
            properties=MYSQL_JDBC_PROPERTIES,
        )
    )

    print(
        f"[RAW BATCH {batch_id}] "
        f"{row_count} transaksi (Join & Feature Engineering) tersimpan ke MySQL"
    )


query_raw = (
    parsed_stream.writeStream

    .foreachBatch(
        write_transactions_to_mysql
    )

    .outputMode("append")

    .trigger(
        processingTime="10 seconds"
    )

    .option(
        "checkpointLocation",
        str(PROJECT_ROOT / "checkpoint" / "raw_transactions")
    )

    .start()
)


# =========================================================
# 6. SALES SUMMARY PER 1 MENIT
# =========================================================

sales_summary_stream = (
    parsed_stream

    .withWatermark(
        "transaction_ts",
        "2 minutes"
    )

    .groupBy(
        window(
            col("transaction_ts"),
            "1 minute"
        )
    )

    .agg(
        spark_count(
            "transaction_id"
        ).alias(
            "total_transactions"
        ),

        spark_sum(
            "quantity"
        ).alias(
            "total_quantity"
        ),

        spark_sum(
            "total_amount"
        ).alias(
            "total_revenue"
        ),
    )
)


def write_summary_to_mysql(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    output_df = (
        batch_df

        .select(
            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            col("total_transactions"),

            col("total_quantity"),

            col("total_revenue"),
        )
    )

    (
        output_df.write
        .jdbc(
            url=MYSQL_JDBC_URL,
            table="sales_summary",
            mode="append",
            properties=MYSQL_JDBC_PROPERTIES,
        )
    )

    print(
        f"[SALES SUMMARY {batch_id}] "
        "Data tersimpan"
    )


query_summary = (
    sales_summary_stream.writeStream

    .foreachBatch(
        write_summary_to_mysql
    )

    .outputMode("append")

    .trigger(
        processingTime="30 seconds"
    )

    .option(
        "checkpointLocation",
        str(PROJECT_ROOT / "checkpoint" / "sales_summary")
    )

    .start()
)


# =========================================================
# 7. CATEGORY SUMMARY
# =========================================================

category_summary_stream = (
    parsed_stream

    .withWatermark(
        "transaction_ts",
        "2 minutes"
    )

    .groupBy(
        window(
            col("transaction_ts"),
            "1 minute"
        ),

        col("category")
    )

    .agg(
        spark_sum(
            "quantity"
        ).alias(
            "total_quantity"
        ),

        spark_sum(
            "total_amount"
        ).alias(
            "total_revenue"
        ),
    )
)


def write_category_summary_to_mysql(
    batch_df,
    batch_id
):

    if batch_df.rdd.isEmpty():
        return

    output_df = (
        batch_df

        .select(
            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            col("category"),

            col("total_quantity"),

            col("total_revenue"),
        )
    )

    (
        output_df.write
        .jdbc(
            url=MYSQL_JDBC_URL,
            table="category_summary",
            mode="append",
            properties=MYSQL_JDBC_PROPERTIES,
        )
    )

    print(
        f"[CATEGORY SUMMARY {batch_id}] "
        "Data tersimpan"
    )


query_category = (
    category_summary_stream.writeStream

    .foreachBatch(
        write_category_summary_to_mysql
    )

    .outputMode("append")

    .trigger(
        processingTime="30 seconds"
    )

    .option(
        "checkpointLocation",
        str(PROJECT_ROOT / "checkpoint" / "category_summary")
    )

    .start()
)


# =========================================================
# 8. PAYMENT SUMMARY
# =========================================================

payment_summary_stream = (
    parsed_stream

    .withWatermark(
        "transaction_ts",
        "2 minutes"
    )

    .groupBy(
        window(
            col("transaction_ts"),
            "1 minute"
        ),

        col("payment_method")
    )

    .agg(
        spark_count(
            "transaction_id"
        ).alias(
            "total_transactions"
        ),

        spark_sum(
            "total_amount"
        ).alias(
            "total_revenue"
        ),
    )
)


def write_payment_summary_to_mysql(
    batch_df,
    batch_id
):

    if batch_df.rdd.isEmpty():
        return

    output_df = (
        batch_df

        .select(
            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            col("payment_method"),

            col("total_transactions"),

            col("total_revenue"),
        )
    )

    (
        output_df.write
        .jdbc(
            url=MYSQL_JDBC_URL,
            table="payment_summary",
            mode="append",
            properties=MYSQL_JDBC_PROPERTIES,
        )
    )

    print(
        f"[PAYMENT SUMMARY {batch_id}] "
        "Data tersimpan"
    )


query_payment = (
    payment_summary_stream.writeStream

    .foreachBatch(
        write_payment_summary_to_mysql
    )

    .outputMode("append")

    .trigger(
        processingTime="30 seconds"
    )

    .option(
        "checkpointLocation",
        str(PROJECT_ROOT / "checkpoint" / "payment_summary")
    )

    .start()
)


# =========================================================
# 9. STATUS
# =========================================================

print("")
print("=" * 60)
print("SEMUA STREAMING QUERY BERHASIL DIMULAI")
print("Input  : Semi-Structured JSON")
print("Output : Structured Data")
print("Kafka -> Spark -> Structured Data -> MySQL")
print("Tekan Ctrl+C untuk menghentikan Spark.")
print("=" * 60)


# =========================================================
# 10. TUNGGU STREAMING
# =========================================================

spark.streams.awaitAnyTermination()