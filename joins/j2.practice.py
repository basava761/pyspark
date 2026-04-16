from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

# Employees DataFrame
emp_data = [
    (1, "A", 10),
    (2, "B", 20),
    (3, "C", 20),   # duplicate dept
    (4, "D", 30)
]

df1 = spark.createDataFrame(emp_data, ["emp_id", "name", "dept_id"])


# Departments DataFrame
dept_data = [
    (10, "HR"),
    (20, "IT"),
    (20, "IT_DUP"),   # duplicate dept_id
    (30, "Finance")
]

df2 = spark.createDataFrame(dept_data, ["dept_id", "dept_name"])

print("=== EMPLOYEES ===")
df1.show()

print("=== DEPARTMENTS ===")
df2.show()
#==========================================================================
print("Perform INNER JOIN")
df1.join(df2,'dept_id','inner').select('emp_id','name','dept_name').show()

print("👉 Find number of employees in each department,Aggregation After Join")
df1.join(df2,'dept_id','inner').select('emp_id','dept_name','dept_id').groupBy('dept_id')

from pyspark.sql.functions import coalesce, lit

df2_clean = df2.dropDuplicates(["dept_id"])

df1.join(df2_clean, "dept_id", "left") \
   .select(
       coalesce("dept_name", lit("Unknown")).alias("dept_name")
   ) \
   .groupBy("dept_name") \
   .count() \
   .show()