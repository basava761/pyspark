#from pyspark import SparkContext
#sc=SparkContext('local','test')
#data=[
#("North", "Laptop", 2, 1200)
#("South", "Laptop", 1, 1150)
#("North", "Phone", 3, 800)
#("East", "Laptop", 4, 1250)
#("South", "Phone", 2, 750)
#("East", "Phone", 5, 820)
#("North", "Laptop", 1, 1180)
#("South", "Laptop", 2, 1170)
#]

#rdd=sc.parallelize(data)
#r=rdd.map(lambda x:(((x[0],x[1]),x[2]),x[4]))
#r1=r.reducedByKey(lambda x,y:x[0],y[1],(x[2]+y[2]),(x[4/x[2]])).collect()
#print(r1)

from pyspark import SparkContext

sc = SparkContext("local", "RDD Aggregation")

data = [
    ("North", "Laptop", 2, 1200),
    ("South", "Laptop", 1, 1150),
    ("North", "Phone", 3, 800),
    ("East", "Laptop", 4, 1250),
    ("South", "Phone", 2, 750),
    ("East", "Phone", 5, 820),
    ("North", "Laptop", 1, 1180),
    ("South", "Laptop", 2, 1170)
]

rdd = sc.parallelize(data)

# Key: (Region, Product)
# Value: (Quantity, Price, Revenue)
pairs = rdd.map(lambda x: ((x[0], x[1]), (x[2], x[3], x[2]*x[3])))

# Aggregate using reduceByKey
agg = pairs.reduceByKey(
    lambda a, b: (
        a[0] + b[0],   # total quantity
        (a[1] + b[1]) / 2,  # average price (simplified, not weighted)
        a[2] + b[2]    # total revenue
    )
)

print(agg.collect())
#[(('North', 'Laptop'), (3, 1190.0, 3580)), (('South', 'Laptop'), (3, 1160.0, 3490)), (('North', 'Phone'), (3, 800, 2400)), (('East', 'Laptop'), (4, 1250, 5000)), (('South', 'Phone'), (2, 750, 1500)), (('East', 'Phone'), (5, 820, 4100))]

