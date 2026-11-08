from pyspark.sql import SparkSession

MYSQL_URL = (
    "jdbc:mysql://localhost:3306/sales_dashboard"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

MYSQL_PROPERTIES = {
    "user": "root",
    "password": "dafa",
    "driver": "com.mysql.cj.jdbc.Driver",
}

spark = (
    SparkSession.builder
    .appName("TestMySQLConnection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========== TEST SPARK -> MYSQL ==========\n")

df = (
    spark.read
    .jdbc(
        url=MYSQL_URL,
        table="transactions",
        properties=MYSQL_PROPERTIES,
    )
)

print("Koneksi Spark -> MySQL BERHASIL!")
print(f"Jumlah data: {df.count()}")

df.show(10, truncate=False)

spark.stop()

print("\n========== TEST SELESAI ==========\n")