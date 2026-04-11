from pyspark import SparkContext
sc=SparkContext('local','testapp')

sales_data = [
    ("North", 1000),
    ("South", 1500),
    ("East", 1200),
    ("West", 1700),
    ("North", 800),
    ("South", 700),
    ("East", 600),
    ("West", 900),
]

rdd= sc.parallelize(sales_data)
r1=rdd.reduceByKey(lambda x,y:x+y).collect()
print(r1) # [('North', 1800), ('South', 2200), ('East', 1800), ('West', 2600)]

sales_data = [
    ("North", "Electronics", 1000),
    ("South", "Electronics", 1500),
    ("East", "Furniture", 1200),
    ("West", "Furniture", 1700),
    ("North", "Electronics", 800),
    ("South", "Furniture", 700),
    ("East", "Furniture", 600),
    ("West", "Electronics", 900),
    ("North", "Furniture", 500),
    ("South", "Electronics", 1200),
]

r = sc.parallelize(sales_data)

r1=r.map(lambda x:((x[0],x[1]),x[2])).reduceByKey(lambda a,b:a+b).collect()
print(r1) #[(('North', 'Electronics'), 1800), (('South', 'Electronics'), 2700), (('East', 'Furniture'), 1800), (('West', 'Furniture'), 1700), (('South', 'Furniture'), 700), (('West', 'Electronics'), 900), (('North', 'Furniture'), 500)]
