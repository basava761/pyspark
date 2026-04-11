from pyspark.sql import SparkSession
import numpy as np

spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext


x=[{'name':'raj','age':30},{'name':'rajA','age':35},{'name':'basava','age':40}]
#c=['name','age']

df=spark.createDataFrame(x)
df.show()
df.count()
df.printSchema()

#creating data frame from Rdd=================================

x=np.arange(1,10)
r=sc.parallelize(x)
#r1=r.map(lambda x:(x,1))sometimes this issue happens becaust of numpy int64
r1 = r.map(lambda x: (int(x), 1))
df1=spark.createDataFrame(r1,['number', 'value'])
df1.show()
#====================================Row=========================
from pyspark.sql import Row

row = Row(name="Basava", age=25)

print(row)