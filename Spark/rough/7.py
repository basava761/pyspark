from pyspark import SparkContext
sc=SparkContext('local','app')
sales_data = [
    ("StoreA", "Shoes", 200),
    ("StoreB", "Shoes", 150),
    ("StoreA", "Shoes", 300),
    ("StoreA", "Bags", 400),
    ("StoreB", "Bags", 250),
    ("StoreA", "Bags", 350),
    ("StoreB", "Shoes", 100),
    ("StoreA", "Shoes", 250),
]
#rdd=sc.parallelize(sales_data)
#r=rdd.map(lambda x:((x[0],x[1]),x[2]))
#total_sales=r.reduceByKey(lambda a,b:a+b)
#min_tran=total_sales.reduceByKey(lambda x,y:min(x,y))
#tt=total_sales=total_sales(lambda x:count(x))
#print(total_sales)
#print(min_tran)
#print(tt)
rdd = sc.parallelize(sales_data)

# Map each record to ((store, category), (sales, 1, sales))
r = rdd.map(lambda x: ((x[0], x[1]), (x[2], 1, x[2])))

# Reduce by key: sum sales, count transactions, track max transaction
result = r.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], max(a[2], b[2]))).collect()

print(result)#[(('StoreA', 'Shoes'), (750, 3, 300)), (('StoreB', 'Shoes'), (250, 2, 150)), (('StoreA', 'Bags'), (750, 2, 400)), (('StoreB', 'Bags'), (250, 1, 250))]
