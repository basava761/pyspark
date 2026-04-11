from pyspark import SparkContext
sc=SparkContext('local','test')

data = [("A", 1), ("B", 2), ("A", 3)]
rdd = sc.parallelize(data)
print(rdd.collect())
sum=rdd.reduceByKey(lambda a,b:a+b).collect()
print(sum)
r2=rdd.map(lambda x:(x[0],(x[1],1)))
res=r2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).collect()
print(res) #[('A', (4, 2)), ('B', (2, 1))]

