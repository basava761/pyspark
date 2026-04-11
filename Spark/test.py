from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkValidationTest") \
    .getOrCreate()

# Create a small test DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Perform a simple operation
df.groupBy("Age").count().show()

# Stop the Spark session
spark.stop()
