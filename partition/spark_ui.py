from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Spark_UI_Master_Lab") \
    .getOrCreate()

print("Spark UI:", spark.sparkContext.uiWebUrl)

# Create large dataset
df = spark.range(0, 1000000) \
    .withColumn("department", (col("id") % 3)) \
    .withColumn("salary", col("id") * 10)

print("Initial Partitions:", df.rdd.getNumPartitions())

# Trigger shuffle
df2 = df.repartition(8, "department")

print("After Repartition:", df2.rdd.getNumPartitions())

# Expensive transformation
result = df2.groupBy("department") \
            .sum("salary")

# Cache the result
result.cache()

print("Storage level before action:",
      result.storageLevel)

# Action 1 → Materialize cache
print("Count:", result.count())

# Action 2 → Reuse cache
result.show()

# Action 3 → Reuse cache again
result.collect()

print("Storage level after cache:",
      result.storageLevel)

input("Spark UI is active. Press Enter to exit...")

spark.stop()