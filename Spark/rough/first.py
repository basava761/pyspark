from pyspark import SparkContext

sc=SparkContext('local','test')

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

rdd= sc.parallelize(sales_data)

r=rdd.map(lambda x:((x[0],x[1]),x[2])).reduceByKey(lambda a,b:max(a,b)).collect()
print(r)#[(('North', 'Electronics'), 1000), (('South', 'Electronics'), 1500), (('East', 'Furniture'), 1200), (('West', 'Furniture'), 1700), (('South', 'Furniture'), 700), (('West', 'Electronics'), 900), (('North', 'Furniture'), 500)]
