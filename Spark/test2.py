from pyspark.sql import SparkSession 
import numpy as np

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkValidationTest") \
    .getOrCreate()

sc=spark.sparkContext
print("==============================================================================================================================================")
x=np.arange(1,100)
rdd=sc.parallelize(x)
print(rdd.collect())
rdd1=rdd.map(lambda x:x*2)
print(rdd1.collect())
print("===========================================================================================================================================")
