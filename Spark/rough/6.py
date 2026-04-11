from pyspark import SparkContext
sc=SparkContext('local','average')
salaries = [
    ("HR", 40000),
    ("IT", 60000),
    ("Finance", 55000),
    ("HR", 45000),
    ("IT", 65000),
    ("Finance", 50000),
    ("HR", 42000),
]
r= sc.parallelize(salaries)
rdd=r.map(lambda x:(x[0],(x[1],1)))

r1=rdd.reduceByKey(lambda a,b:(a[0]+b[0],a[1],b[1]))
avg=r1.mapValues(lambda x:x[0]/x[1]).collect()
print(avg) #[('HR', 127000.0), ('IT', 125000.0), ('Finance', 105000.0)]
