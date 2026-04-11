from pyspark import SparkContext
sc=SparkContext('local','test')

scores = [
    ("Alice", 85),
    ("Bob", 70),
    ("Alice", 90),
    ("Bob", 80),
    ("Charlie", 95),
]
rdd=sc.parallelize(scores)
r=rdd.map(lambda x: (x[0],x[1]))
r1=r.reduceByKey(lambda a,b:min(a,b)).collect()
print(r1)