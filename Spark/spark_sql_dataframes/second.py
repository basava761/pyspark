from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('test').getOrCreate()

c=['ename','age']
x=[('basava',34),('shivakumar',30)]
df=spark.createDataFrame(x,c)
df.show()
df.count()
df.printSchema()