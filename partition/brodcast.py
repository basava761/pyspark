from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Broadcast_Join_Lab") \
    .getOrCreate()

print("Spark UI:", spark.sparkContext.uiWebUrl)

# Large fact table
fact_df = spark.range(0, 1000000) \
    .withColumn("dept_id", col("id") % 3)

# Small dimension table
dim_data = [
    (0, "IT"),
    (1, "HR"),
    (2, "Finance")
]

dim_df = spark.createDataFrame(
    dim_data,
    ["dept_id", "department"]
)

# Force broadcast
result = fact_df.join(
    broadcast(dim_df),
    "dept_id"
)

result.count()

input("Press Enter to exit...")