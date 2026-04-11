from pyspark import SparkContext
sc=SparkContext('local','app')

sales = [
    ("North", 1000),
    ("South", 1500),
    ("East", 1200),
    ("West", 1700),
    ("North", 800),
    ("South", 700),
    ("East", 600),
    ("West", 900),
]
rdd=sc.parallelize(sales)

r=rdd.map(lambda x:(x[0],x[1]),1)
r1=r.reduceByKey(lambda x,y:x+y).collect()
print(r1) # [('North', 1800), ('South', 2200), ('East', 1800), ('West', 2600)]
