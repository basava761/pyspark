from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('dup_prac').getOrCreate()

data = [
    (1,'John','IT',50000,'john@gmail.com'),
    (2,'Ravi','HR',45000,'ravi@gmail.com'),
    (3,'Sam','Finance',55000,'sam@gmail.com'),
    (4,'David','IT',60000,'david@gmail.com'),

    (1,'John','IT',50000,'john@gmail.com'),

    (5,'Mike','HR',48000,'ravi@gmail.com'),

    (6,'John','IT',70000,'john2@gmail.com'),

    (3,'Sam','Finance',55000,'sam@gmail.com'),

    (7,None,'IT',52000,None),
    (8,None,'IT',52000,None),

    (9,'','HR',45000,''),
    (10,'','HR',45000,''),

    (11,'Asha','Finance',65000,'asha@gmail.com'),
    (11,'Asha','Finance',65000,'asha@gmail.com')
]

columns = [
    "emp_id",
    "emp_name",
    "department",
    "salary",
    "email"
]

df = spark.createDataFrame(data, columns)
df.show()
df.persist()
df.agg(count('*').alias('total_count()')).show()
df.filter(
    (col('emp_name').isNull())&
    (col('email').isNull())).select('*').show()

df.filter(
    (col('emp_name')=="")&
    (col('email')=="")).select('*').show()

#Using the table above, write a PySpark query to identify duplicate emp_id values.
#with cte as(select emp_id,count(emp_id) as tcount from employees_dup group by emp_id)
#select emp_id from cte where tcount>1;
df.groupBy('emp_id').agg(count('*').alias('tcount')).filter(col('tcount')>1).show()



#df.groupBy("emp_id").agg(count("*").alias("tcount")).filter(col("tcount") > 1).show()

#Find duplicate email values.
#select email ,count(*) from employees_dup group by email having count(*)>1;
#df.groupby('email').agg(count('*').alias('tcount')).filter(col('tcount')>1).show()


df.groupBy("email").agg(count("*").alias("tcount")).filter(col("tcount") > 1).show()

#Find exact duplicate rows in the entire table
#mysql> SELECT     emp_id,     emp_name,     department,     salary,     email,     COUNT(*) AS tcount FROM employees_dup GROUP BY     emp_id,     emp_name,     department,     salary,     email HAVING COUNT(*) > 1;

#df.groupBy('df.columns').count().filter('count')>1

#Count the number of duplicate emp_id records.

#mysql> select emp_id ,count(*)-1 from employees_dup group by emp_id;
df.groupBy('emp_id').agg((count('*')-1).alias('total_count')).filter(count('*')>0).show()

from pyspark.sql.functions import count, col

df.groupBy("emp_id").agg((count("*") - 1).alias("duplicate_count")).filter(col("duplicate_count") > 0).show()



