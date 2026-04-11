from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")


emp_data = [
    (1, "A", 10),
    (2, "B", 20),
    (3, "C", 30),
    (4, "D", 40)
]

df1 = spark.createDataFrame(emp_data, ["emp_id", "name", "dept_id"])
df1.show()

#+------+----+-------+
#|emp_id|name|dept_id|
#+------+----+-------+
#|     1|   A|     10|
#|     2|   B|     20|
#|     3|   C|     30|
#|     4|   D|     40|
#+------+----+-------+


dept_data = [
    (10, "HR"),
    (20, "IT"),
    (30, "Finance"),
    (50, "Admin")
]

df2= spark.createDataFrame(dept_data, ["dept_id", "dept_name"])
df2.show()

#+-------+---------+
#|dept_id|dept_name|
#+-------+---------+
#|     10|       HR|
#|     20|       IT|
#|     30|  Finance|
#|     50|    Admin|
#+-------+---------+
#syntax for joins
#df1.join(df2, "id", "inner")
#df1.join(df2, "id", "left")
#df1.join(df2, "id", "right")
#df1.join(df2, "id", "outer")
#df1.join(df2, "id", "left_semi")
#df1.join(df2, "id", "left_anti")
#df1.join(df2, df1.id == df2.id, "inner")
#df1.join(df2, df1.id == df2.id, "left")
#df1.join(df2, df1.id == df2.id, "right")
#df1.join(df2, df1.id == df2.id, "outer")
#df1.join(df2, df1.id == df2.id, "left_semi")
#df1.join(df2, df1.id == df2.id, "left_anti")
#df1.crossJoin(df2)
df1.join(df2,'dept_id','inner').show()

#+-------+------+----+---------+
#|dept_id|emp_id|name|dept_name|
#+-------+------+----+---------+
#|     10|     1|   A|       HR|
#|     20|     2|   B|       IT|
#|     30|     3|   C|  Finance|
#+-------+------+----+---------+

df1.join(df2,'dept_id','left').show()

#+-------+------+----+---------+
#|dept_id|emp_id|name|dept_name|
#+-------+------+----+---------+
#|     10|     1|   A|       HR|
#|     20|     2|   B|       IT|
#|     30|     3|   C|  Finance|
#|     40|     4|   D|     NULL|
#+-------+------+----+---------+

df1.join(df2,'dept_id','right').show()
#+-------+------+----+---------+
#|dept_id|emp_id|name|dept_name|
#+-------+------+----+---------+
#|     10|     1|   A|       HR|
#|     20|     2|   B|       IT|
#|     30|     3|   C|  Finance|
#|     50|  NULL|NULL|    Admin|
#+-------+------+----+---------+

c = df1.crossJoin(df2).cache()

c.count()
c.show()

#------+----+-------+-------+---------+
#|emp_id|name|dept_id|dept_id|dept_name|
#+------+----+-------+-------+---------+
#|     1|   A|     10|     10|       HR|
#|     1|   A|     10|     20|       IT|
#|     1|   A|     10|     30|  Finance|
#|     1|   A|     10|     50|    Admin|
#|     2|   B|     20|     10|       HR|
#|     2|   B|     20|     20|       IT|
#|     2|   B|     20|     30|  Finance|
#|     2|   B|     20|     50|    Admin|
#|     3|   C|     30|     10|       HR|
#|     3|   C|     30|     20|       IT|
#|     3|   C|     30|     30|  Finance|
#|     3|   C|     30|     50|    Admin|
#|     4|   D|     40|     10|       HR|
#|     4|   D|     40|     20|       IT|
#|     4|   D|     40|     30|  Finance|
#|     4|   D|     40|     50|    Admin|
#+------+----+-------+-------+---------+
