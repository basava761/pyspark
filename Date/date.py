from os import truncate

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('date').getOrCreate()

data = [
    {"id": 1, "name": "Alice", "age": 29},
    {"id": 2, "name": "Shiva", "age": 30},
    {"id": 2, "name": "Basava", "age": 35},
]
df=spark.createDataFrame(data)
df.show(n=2,truncate=False,vertical=True)


df2=df.withColumn('today',current_timestamp())
df2.show(truncate=False)

df3=df.withColumn('today',curdate())
df3.show()
df4=df.withColumn('today',date_add(current_date(),15))
df4.show(truncate=False)

#show() accepts three arguments: n (rows to display), 
# truncate (whether/how much to shorten values), and 
# vertical (display records vertically for readability).