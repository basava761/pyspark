from pyspark.sql import SparkSession 
import numpy as np

spark=SparkSession.builder.appName('test').getOrCreate()

sc=spark.sparkContext

x=np.arange(1,10)
r1=sc.parallelize(x)
print(r1.collect())
print(r1.getNumPartitions(),'===========================>')
r2=r1.map(lambda y:y*y)
r2.persist()
print(r2.collect())
r3=r2.filter(lambda o:o%2!=0)
print(r3.collect())

r4=r3.map(lambda u: u*10)
print(r4.collect())
r2.unpersist()
print(r4.getNumPartitions(),'===========================>')
