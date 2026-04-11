from pyspark import SparkContext

sc=SparkContext('local','grouping and aggregation')

data = [
    ("Alice", "Math", 85),
    ("Bob", "Math", 70),
    ("Alice", "Science", 90),
    ("Bob", "Science", 80),
    ("Charlie", "Math", 95),
]

rdd = sc.parallelize(data)
r2=rdd.map(lambda x:(x[1],x[2])).groupByKey().mapValues(list)


r2 = rdd.map(lambda x: (x[1], x[2]))
totals = r2.reduceByKey(lambda a, b: a + b).collect()
print(totals) # [('Math', 250), ('Science', 170)]




