from pyspark.sql import SparkSession


spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext
import numpy as np

x=np.arange(1,10)
r=sc.parallelize(x)
print(r.collect()) #[1, 2, 3, 4, 5, 6, 7, 8, 9]

print (r.count()) # 9
print(r.take(5)) #[1, 2, 3, 4, 5]
#--------------------------------------------countBy value which yield python dict============================= 
med=['ind','aus','nZ','ind','aus','nZ','ind','aus','nZ']
rdd=sc.parallelize(med)
c=rdd.countByValue()
print(c,'count by value============================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')#{'ind': 3, 'aus': 3, 'nZ': 3}) count by value============================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>