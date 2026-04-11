from pyspark import SparkContext

sc=SparkContext('local','testapp')
"""
first we ned to map to make it k,v then we need to applyreduceByKey()

"""
w = ["apple", "banana", "apple", "orange", "banana", "apple"]
r=sc.parallelize(w)
r1=r.map(lambda x:(x,1))
r2=r1.reduceByKey(lambda a,b:a+b).collect()
print(r2) #[('apple', 3), ('banana', 2), ('orange', 1)]

