from pyspark.sql import SparkSession
import numpy as np

spark=SparkSession.builder.appName('demo-test').getOrCreate()

sc=spark.sparkContext

#increasing or decreasing packages_distributions
x=np.arange(0,100,10)

r1=sc.parallelize(x,1)

print (r1.collect())
print(r1.getNumPartitions(),'*******************************partitions==================')#4 by default

r2=sc.parallelize(x,10)
print(r2.getNumPartitions(),'***********************************************************')#10

r3=r2.coalesce(1)
print(r3.getNumPartitions(),'}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}')

r1.saveAsTextFile("hdfs://localhost:9000/test/partitions/demo")