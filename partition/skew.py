from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark=SparkSession.builder.appName('skew').getOrCreate()



data = [
    (101,"John","IT","Bangalore",60000,"2025-01-15"),
    (102,"Mike","IT","Hyderabad",75000,"2025-01-16"),
    (103,"Sara","IT","Chennai",75000,"2025-01-17"),
    (104,"David","HR","Mumbai",45000,"2025-02-01"),
    (105,"Emma","HR","Delhi",55000,"2025-02-03"),
    (106,"James","Finance","Pune",90000,"2025-03-10"),
    (107,"Sophia","Finance","Bangalore",85000,"2025-03-11"),
    (108,"Liam","Finance","Hyderabad",85000,"2025-03-12"),
    (109,"Olivia","IT","Chennai",50000,"2025-04-01"),
    (110,"Noah","HR","Mumbai",45000,"2025-04-02")
]

cols = [
    "emp_id",
    "emp_name",
    "department",
    "city",
    "salary",
    "joining_date"
]

df = spark.createDataFrame(data, cols)

df.show()
#Check how many partitions this DataFrame currently has.
print(df.rdd.getNumPartitions())

#Suppose you want to increase the partitions to 5.
df2=df.repartition(5)

print(df2.rdd.getNumPartitions())

df2.withColumn('partition_id',spark_partition_id())

#Create 3 partitions based on department.
df3=df2.repartition(3,'department')
print(df2.repartition(3,'department'))
df3.withColumn('partition_id',spark_partition_id()).show()
