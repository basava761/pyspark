from pyspark.sql import SparkSession
import numpy as np
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

spark=SparkSession.builder.appName('test').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

"""
spark = SparkSession.builder \
    .appName('test') \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console") \
    .getOrCreate()
"""

sc=spark.sparkContext
"""======================================================
RDD operations:
transformationa:
actions
"""
x=np.arange(1,10)
y=np.arange(4,9)
#============================================Map==========================================================
r=sc.parallelize(x)
y1=sc.parallelize(y)
print(r.collect())
print(y1.collect())
r2=r.map(lambda n:n*n)# squaring each element of r
print(r2.collect())
print(r2.getNumPartitions(),"===========>partitions")

r3=r.map(lambda s:s+5)#adding 5 to each element of r
print(r3.collect())

#flat map========================<>
f=[[10,20,30],[40,50,60],[70,80,90]]
rf=sc.parallelize(f)
fm=rf.flatMap(lambda x:x)
print(fm.collect(),"======flatmap=========>")

#==============================filter=====================================================================
sal=np.arange(10000,90000,10000)
rdd=sc.parallelize(sal)
print(rdd.collect())
rdd1=rdd.filter(lambda c:c>20000)
print(rdd1.collect()),"filterred values above 20k"

subjects = [
    "Big Data Fundamentals",
    "Hadoop HDFS",
    "YARN",
    "MapReduce",
    "Hadoop 1.x vs 2.x",
    "Spark RDD",
    "Spark Transformations & Actions",
    "Lazy Evaluation",
    "DAG & Lineage",
    "Partitioning",
    "Persistence & Caching",
    "Broadcast Variables",
    "Accumulators",
    "Shuffle Mechanism",
    "Joins in Spark",
    "Repartition vs Coalesce",
    "Data Skew Handling",
    "Spark Memory Management",
    "Serialization (Kryo)",
    "Spark SQL",
    "DataFrames & Datasets",
    "Catalyst Optimizer",
    "Tungsten Engine",
    "Window Functions",
    "UDF vs Built-in Functions",
    "Structured Streaming",
    "File Formats (Parquet, ORC, Avro)",
    "Compression Techniques",
    "BigQuery",
    "GCS",
    "Dataflow",
    "Pub/Sub",
    "Dataproc",
    "SQL Joins",
    "SQL Window Functions",
    "CTEs & Subqueries",
    "ETL vs ELT",
    "Data Pipelines",
    "Schema Design",
    "Failure Scenarios",
    "Performance Tuning",
    "Python Basics for Data Engineering",
    "Real-world Use Cases"
]
s=sc.parallelize(subjects)
rdd=s.filter(lambda x:x!='GCS')
print(rdd.collect())

#============================UNION=========================================
#union: combines elements of multiple rdds by default it performs uniall opration
r1=sc.parallelize([10,20,30,40,50])
r2=sc.parallelize([10,20,30,60,70])
res=r1.union(r2).collect()
print(res,'ubion elements') #[10, 20, 30, 40, 50, 10, 20, 30, 60, 70]

#============================INTERSECTION=========================================
"""RETURNS ONLY MATCH ELEMENTS FROM BOTH RDD"""
r3=r1.intersection(r2).collect()
print(r3,'intersection_elements...........')#[10, 20, 30] intersection_elements...........
#=============================suntract==========================================

r4=r2.subtract(r1).collect()
r5=r1.subtract(r2).collect()
print(r4) #[60, 70]
print(r5) #[40, 50]

#======================cartisian===========================
r10=r1.cartesian(r2).collect()
r11=r2.cartesian(r1).collect()
print(r10) #[(10, 10), (10, 20), (10, 30), (10, 60), (10, 70), (20, 10), (20, 20), (20, 30), (20, 60), (20, 70), (30, 10), (30, 20), (30, 30), (30, 60), (30, 70), (40, 10), (50, 10), (40, 20), (50, 20), (40, 30), (50, 30), (40, 60), (40, 70), (50, 60), (50, 70)]
print(r11) #[(10, 10), (10, 20), (10, 30), (10, 40), (10, 50), (20, 10), (20, 20), (20, 30), (20, 40), (20, 50), (30, 10), (30, 20), (30, 30), (30, 40), (30, 50), (60, 10), (70, 10), (60, 20), (70, 20), (60, 30), (70, 30), (60, 40), (60, 50), (70, 40), (70, 50)]


#======================distinct===========================

d=[10,67,76,67,98,98,89,10,67,76,67,98,98,89]
dr=sc.parallelize(d)
r=dr.distinct().collect()
print (r,'distinctresults')

"""applying multiple tranaformations"""
