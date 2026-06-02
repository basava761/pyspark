from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("FilterPractice").getOrCreate()

# Sample data
data = [
    ("Alice", 24, "Delhi", 85),
    ("Bob", 30, "Mumbai", 90),
    ("Charlie", 18, "Delhi", 70),
    ("David", 35, "Chennai", 88),
    ("Eva", 28, "Bangalore", 95)
]

columns = ["Name", "Age", "City", "Score"]

df = spark.createDataFrame(data, columns)

df.show()

#Filter all people who live in Delhi.

df.select('name').filter(col('City')=='Delhi').show()
#df.select('name').filter(col('City')=='Delhi').show()
df.filter(col('age')>25).select('*').show()

#In Python, the and operator will evaluate immediately and return a single boolean (True or False), not a Spark expression.

#Spark requires bitwise operators (& for AND, | for OR) to combine column conditions.
print("Filter all people whose Age is greater than 25 AND Score is greater than 85.")

df.select('*').filter((col('age')>25)&(col('Score')>85)).show()

print("Filter all people whose City is either Delhi or Chennai.")
df.filter((col('City')=='Delhi')|(col('city')=='Chennai')).select('*').show()

#================================================================================
#Filter all people whose Name contains the letter "a" (case-insensitive).
#df.select('*').filter(col('Name') like %'a'%).show()
df.filter(col('Name').like("%a%")).select('*').show()
df.filter(col('Name').contains('a')&(col('Score')>80)).show()

#between
#Filter all people whose Score is between 80 and 90 (inclusive).
df.select('*').filter(col('Score').between(80 , 90)).show()

# Select rows where Age < 30 AND Score > 80
df.select('*').filter((col('age')<30) &(col('Score')>90)).show()

df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE Age < 30 AND Score > 80").show()

#Filter all people whose Name starts with 'A' or ends with 'a' using regex (rlike).
df.select('*').filter((col('Name').rlike('^A'))|(col('Name').rlike('a$'))).show()
#===========================================================================
#===========================================================================
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

windowSpec = Window.orderBy(col("Score").desc())

df.withColumn("rank", row_number().over(windowSpec)) \
.filter(col("rank") <= 2) \
.show()

#We want to find the top scorer in each City.
w=Window.partitionBy('City').orderBy(col('Score').desc())
df.withColumn('rank',row_number().over(w)).filter(col('rank')<=2).show()

# Task: Find all cities where the average Score > 85.
df.groupBy('City').agg(avg(col('Score')).alias('average_score')).filter(col('average_score')>80).show()

