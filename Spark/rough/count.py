from pyspark import SparkContext
sc=SparkContext('local','count')

data = [("X", 5), ("Y", 10), ("X", 15)]
rdd = sc.parallelize(data)

r=rdd.map(lambda x:(x[0],(x[1],1)))

sum_c=r.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).collect()
print(sum_c) #[('X', (20, 2)), ('Y', (10, 1))]
