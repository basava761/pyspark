from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName('partition').getOrCreate()


data = [
    (1,"John","IT","Bangalore",75000),
    (2,"Mary","HR","Hyderabad",65000),
    (3,"David","IT","Bangalore",85000),
    (4,"Smith","Finance","Chennai",90000),
    (5,"Alice","IT","Hyderabad",70000),
    (6,"Bob","HR","Mumbai",60000),
    (7,"Tom","Finance","Bangalore",95000),
    (8,"Emma","IT","Mumbai",72000),
    (9,"Chris","HR","Chennai",58000),
    (10,"Sophia","Finance","Hyderabad",88000),
    (11,"Raj","IT","Bangalore",76000),
    (12,"Anu","HR","Mumbai",62000),
    (13,"Vijay","Finance","Chennai",91000),
    (14,"Priya","IT","Hyderabad",79000),
    (15,"Kiran","HR","Bangalore",64000),
    (16,"Neha","Finance","Mumbai",93000),
    (17,"Arjun","IT","Chennai",81000),
    (18,"Meera","HR","Hyderabad",67000),
    (19,"Ravi","Finance","Bangalore",97000),
    (20,"Pooja","IT","Mumbai",74000)
]

columns = ["emp_id","name","department","city","salary"]

df = spark.createDataFrame(data, columns)

#df.explain(True)

#The downstream processing needs more parallelism. Increase the DataFrame from 4 partitions to 8 partitions and verify the new partition count.
#df2=df.repartition(2)
#print(df2.rdd.getNumPartitions())
#from pyspark.sql.functions import spark_partition_id

#df2.withColumn("partition_id", spark_partition_id()) \
#  .show(truncate=False)
#==================================================================
#Start from df2 (which has 8 partitions).
#Reduce the partitions to 3.
#Use the most appropriate Spark method.
#Verify the new partition count.
#Display the partition IDs to confirm the distribution.
#-------------------------------------------------------------------------
df2=df.repartition(8)
print(df2.rdd.getNumPartitions())

df3=df2.repartition(3)
print(df3.rdd.getNumPartitions())

df3.withColumn('spark_id',spark_partition_id()).show()
#--------------------------------------------reduced to 5 files
#df2=df.repartition()
print(df2.rdd.getNumPartitions())

df4=df3.coalesce(5)
print(df4.rdd.getNumPartitions())

df3.withColumn('spark_id',spark_partition_id()).show()
#----------------------------------------------------------------------------
print("Spark UI:", spark.sparkContext.uiWebUrl)
#---------------------------------------------------------------------------------
print("Spark UI:", spark.sparkContext.uiWebUrl)

df = spark.range(1000000)

df.count()

input("Press Enter to exit...")