from pyspark.sql import SparkSession 
import numpy as np
"""
map ,flatmap,filter
"""

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkValidationTest") \
    .getOrCreate()

sc=spark.sparkContext

x=np.arange(1,20)

r1=sc.parallelize(x)
print("====================>",r1.collect())
r2=r1.map(lambda x:x*x*x)
print("====================>",r2.collect())
r3=r2.filter(lambda y:y%2==0)
print("====================>",r3.collect())
