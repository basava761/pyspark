from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "A", None),
    (2, None, "Fallback"),
    (3, None, None)
]

df = spark.createDataFrame(data, ["id", "name", "backup"])

#✅ Your Task Recap

#Create column final_name:

#use name if present
#else backup
#else "Unknown"



#df.select(
#    "id",
#    "name",
#    "backup",
#    coalesce("name", "backup", lit("Unknown")).alias("final_name")
#).show()
#====================================================================

data = [
    (1, "A"),
    (2, None)
]

df = spark.createDataFrame(data, ["id", "name"])
df.show()

df.select(
    "id",
    when(col("name").isNotNull(), col("name"))
    .otherwise("Unknown")
).show()
print('with colalesce')
df.select('id',coalesce('name',lit('unknown')).alias('lists')).show()

"""
oalesce(x, y) means:

“Give me x, if x is NULL → give me y”

"""