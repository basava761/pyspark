from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("SparkSQLTest") \
    .getOrCreate()

# Create sample data
data = [
    (1, "Alice", 5000),
    (2, "Bob", 6000),
    (3, "Charlie", 4000)
]

columns = ["emp_id", "emp_name", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Register DataFrame as a temporary SQL view
df.createOrReplaceTempView("employees")

# Run Spark SQL query
result = spark.sql("SELECT emp_id, emp_name FROM employees WHERE salary > 4500")

# Show results
result.show()
