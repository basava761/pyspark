from pyspark.sql import SparkSession
import numpy as np

spark=SparkSession.builder.appName('demo').getOrCreate()

sc=spark.sparkContext

x=np.arange(1,11,2)

r=sc.parallelize(x)
res=r.collect()
print(res) #[1, 3, 5, 7, 9]


#========================================first=============================================
f=r.first() #-->its an acion
print(r.first(),"====================>") # 1

#==================================Reduce============================================
#reduce -->also an action
red=r.reduce(lambda x,y:x+y)
print(red,'#####################################################') # 25
#===================================sum,max,mi,count==============================
print(r.max(),'MAX OF RDD')
print(r.min(),'MIN OF RDD')
print(r.count(),'COUT OF RDD')
print(r.sum(),'SUM OF RDD')
#======================================saveAsTextFile=======================
r.saveAsTextFile("hdfs://localhost:9000/test/demo")

