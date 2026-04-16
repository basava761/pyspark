from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "A", "IT", 50),
    (2, "B", "HR", 40),
    (3, "C", "IT", 60),
    (4, "D", "FN", 45),
    (5, "E", "HR", 42),
    (6, "F", "IT", 70),
    (7, "G", "FN", 48),
    (8, "H", "IT", 55),
    (9, "I", "HR", 39),
    (10, "J", "FN", 52)
]
rdd = sc.parallelize(data)

df = rdd.toDF(["id", "nm", "dp", "sal"])

df.show()
df.printSchema()
df.printSchema()
print(df.columns)

print("Get all employees whose salary is greater than 50")
df.filter(col('sal')>50).show()

print("Show only employee name and salary")
df.select('nm','sal').show()

print("Get employees whose department is 'HR'")
df.filter(col('dp')=="HR").show()

print("Get employees whose salary is between 40 and 60")
#df.select('nm').filter((col('sal'))>40 & (col('sal')<60)).show()
from pyspark.sql.functions import col

df.select("nm") \
.filter((col("sal") > 40) & (col("sal") < 60)) \
.show()
df.select('nm').filter((col('sal')>40)&(col('sal')<60)).show()

print("Sort employees by salary in descending order")
df.sort(desc('sal')).select('nm').show()

print("Count number of employees in each department")
df.groupBy('dp').count().show()

print("Find average salary of employees")
#df.groupBy('id').filter(col(sal/col(id)).count()).show()

df.select(avg('sal')).alias('avg_sal').show()

print("Find maximum salary in each department")
df.groupBy('dp').agg(max('sal')).show()
print("Get employees whose salary is greater than 50 AND department is IT")
df.select('nm','sal').filter((col('sal')<50)(col('dp')=="IT")).show()