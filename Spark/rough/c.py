from pyspark import SparkContext
sc=SparkContext('local','count++')

data = [("P", 2), ("Q", 4), ("P", 6), ("Q", 8), ("P", 10)]
rdd = sc.parallelize(data)

#r=rdd.map(lambda x:(x[0],(x[1],1)))
#c_sum=r.reduceByKey(lambda a,b:(a[0]+b[0],a[1],b[1],max(a[2],b[2]))).collect()
#print(c_sum)

#f you want to track sum, count, max, you need to start with three elements in the map step


r = rdd.map(lambda x: (x[0], (x[1], 1, x[1])))

c_sum = r.reduceByKey(
    lambda a,b: (a[0]+b[0], a[1]+b[1], max(a[2], b[2]))
).collect()

print(c_sum) #[('P', (18, 3, 10)), ('Q', (12, 2, 8))]

