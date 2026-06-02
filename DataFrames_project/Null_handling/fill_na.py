from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("FillNA_Practice").getOrCreate()
data = [
    (1, "John",  "IT",      50000, 101, "john@gmail.com"),
    (2, None,    "HR",      45000, 102, None),
    (3, "Ravi",  None,      None,  101, "ravi@gmail.com"),
    (4, None,    None,      55000, None, None),
    (5, "Sam",   "Finance", None,  103, "sam@gmail.com"),
    (6, None,    "IT",      60000, None, None),
    (7, "David", "HR",      48000, 102, ""),
    (8, "",      "IT",      52000, 101, "david@gmail.com"),
    (9, "Mike",  None,      70000, None, ""),
    (10, None,   "Finance", None,  103, "mike@gmail.com")
]

columns = [
    "emp_id",
    "emp_name",
    "department",
    "salary",
    "manager_id",
    "email"
]

df = spark.createDataFrame(data, columns)

df.show()

#+------+--------+----------+------+--------------+
#|emp_id|emp_name|department|salary|         email|
#+------+--------+----------+------+--------------+
#|     1|    John|        IT| 50000|john@gmail.com|
#|     2|    NULL|        HR| 45000|          NULL|
#|     3|    Ravi|      NULL|  NULL|ravi@gmail.com|
#|     4|    NULL|      NULL| 55000|          NULL|
#|     5|     Sam|   Finance|  NULL| sam@gmail.com|
#|     6|    NULL|        IT| 60000|          NULL|
#+------+--------+----------+------+--------------+
df.printSchema()

#Write a MySQL query to display all rows where emp_name is NULL.

#select * from emp where emp_name is null;
df.select('*').filter(col('emp_name').isNull()).show()

#Display all rows where department is NULL.
#select * from emp where department is  null;
df.filter(col('department').isNull()).show()


#Display all rows where email is NOT NULL.
#select * from emp where email is not null;
df.filter(
    (col("email").isNotNull()) &
    (col("email") == "")
).show()


#Question 6

#Display all rows where:

#department is NOT NULL
#AND
#salary is NOT NULL

df.filter(
    (col('salary').isNotNull()) &
    (col('department').isNotNull())).show()

#uestion 7
#Display all rows where:
#emp_name is NULL
#AND
#email is NULL
df.filter((col('emp_name').isNull())&(col('email').isNull())).show()

#Question 1 (Basic fillna)

#Replace all NULL values in the emp_name column with "Unknown"
df.fillna('Unknown',['emp_name']).show()
df.withColumn('test',when(col('email')=='','empty').otherwise(col('email'))).show()

#Replace NULL values in the salary column with 0
df.withColumn('updated',when(col('salary').isNull(),0).otherwise(col('salary'))).show()


#Replace NULL values in:

#emp_name → "Unknown"
#department → "Not Assigned"
df.fillna({
    'emp_name':'Unknown',
    'department':'Not Assigned',
    #'salary':0
        }).show()

df.fillna(0).show()
df.fillna("NA").show()

df.na.drop().show()

from pyspark.sql.functions import col

print(df.filter(col("email") == "").count())

df.na.drop(subset=['salary']).show()

from pyspark.sql import functions as F

df.select([
    (F.count("*") - F.count(c)).alias(f"{c}_null_count")
    for c in df.columns
]).show()

print(df.columns)