from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext

data = [
    (1, "Arjun", "IT", 50000, "Bangalore", 2),
    (2, "Ravi", "HR", 40000, "Hyderabad", 3),
    (3, "Sneha", "IT", 60000, "Bangalore", 5),
    (4, "Kiran", "Finance", 45000, "Chennai", 4),
    (5, "Meena", "HR", 42000, "Hyderabad", 2),
    (6, "Vikram", "IT", 70000, "Pune", 6),
    (7, "Pooja", "Finance", 48000, "Chennai", 3),
    (8, "Rahul", "IT", 55000, "Bangalore", 4),
    (9, "Anita", "HR", 39000, "Pune", 1),
    (10, "Suresh", "Finance", 52000, "Chennai", 5)
]

columns = ["emp_id", "name", "department", "salary", "city", "experience"]

df = spark.createDataFrame(data, columns)

df.printSchema()

#Find employees whose salary is greater than 50000 AND city is Bangalore
df.filter((col('city')=='Bangalore') & (col('salary')>50000)).select('name').show()
