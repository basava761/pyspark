from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext

print(type(sc))

data = [
    (1, "Arjun", "IT", 50000, "Bangalore", 2),
    (2, "Ravi", "HR", 40000, "Hyderabad", 3),
    (3, "Sneha", "IT", 60000, "Bangalore", 5),
    (4, "Kiran", "Finance", 45000, "Chennai", 4),
    (5, "Meena", "HR", 42000, "Hyderabad", 2),
    (6, "Vikram", "IT", 70000, "Pune", 6),
    (7, "Pooja", "Finance", 48000, "Chennai", 3),
    (8, "Rahul", "IT", 55000, "Bangalore", 4),
    (9, "Anita", "HR", 39000, "Pune", 1),
    (10, "Suresh", "Finance", 52000, "Chennai", 5)
]

columns = ["emp_id", "name", "department", "salary", "city", "experience"]

df = spark.createDataFrame(data, columns)

df.show()
#+------+------+----------+------+---------+----------+
#|emp_id|  name|department|salary|     city|experience|
#+------+------+----------+------+---------+----------+
#|     1| Arjun|        IT| 50000|Bangalore|         2|
#|     2|  Ravi|        HR| 40000|Hyderabad|         3|
#|     3| Sneha|        IT| 60000|Bangalore|         5|
#|     4| Kiran|   Finance| 45000|  Chennai|         4|
#|     5| Meena|        HR| 42000|Hyderabad|         2|
#|     6|Vikram|        IT| 70000|     Pune|         6|
#|     7| Pooja|   Finance| 48000|  Chennai|         3|
#|     8| Rahul|        IT| 55000|Bangalore|         4|
#|     9| Anita|        HR| 39000|     Pune|         1|
#|    10|Suresh|   Finance| 52000|  Chennai|         5|
#+------+------+----------+------+---------+----------+

df1 = df.withColumnRenamed("name", "ename").withColumnRenamed("department", "dept").withColumnRenamed("salary", "sal") \
.withColumnRenamed("experience", "exp")
df1.show()
#to select only required column
#df1.select('ename','dept','exp','sal').show()
#    +------+-------+---+-----+
#| ename|   dept|exp|  sal|
#+------+-------+---+-----+
#| Arjun|     IT|  2|50000|
#|  Ravi|     HR|  3|40000|
#| Sneha|     IT|  5|60000|
#| Kiran|Finance|  4|45000|
#| Meena|     HR|  2|42000|
#|Vikram|     IT|  6|70000|
#| Pooja|Finance|  3|48000|
#| Rahul|     IT|  4|55000|
#| Anita|     HR|  1|39000|
#|Suresh|Finance|  5|52000|
#+------+-------+---+-----+

#Get employees whose salary is greater than average salary
#df1.select('sal').filter(df1.sal >avg(df1.sal)).show()
#avg_sal=df1.agg(avg(df1.sal))
#df1.select('df1.sal'>avg_sal).show()

#avg_sal = df.agg(avg("salary")).first()[0]
#df.filter(df.salary > avg_salary).show()

#Find top 2 highest salary employees in each department
w = Window.partitionBy("dept")

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

#w = Window.partitionBy("department")

#df.withColumn("avg_sal", avg("salary").over(w)) \
#.filter(col("salary") > col("avg_sal")) \
#.show()

#from pyspark.sql.window import Window
#from pyspark.sql.functions import row_number, col

#w = Window.partitionBy("dept").orderBy(col("sal").desc())

#df.withColumn("rank", row_number().over(w)) \
#.filter(col("rank") <= 2) \
#.show()
#--------------------------------------------------------------------------------------
#Get all employees with salary greater than 50000
df.printSchema
#df.filter(df.salary>50000).show()

#Show only employee name and department
#df.select('name','department').show()
df.columns

#df.orderBy(desc('salary')).show()
#alternative=df.orderBy("sal", ascending=False).show()

#Count number of employees in each department
df.groupBy('department').count().show()

#Add a new column "bonus" = 10% of salary
df.withColumn('bonus',df.salary*0.10).show()
#--alt=df.withColumn('bonus,col('salary')*0.10').show()

#Show employees who belong to IT department AND salary > 55000
df.filter((df.department=="IT") & (df.salary>55000)).select('name').show()
#df.filter((df.dept == "IT") & (df.sal > 55000)).select("ename").show()

#Find maximum salary in each department
df.groupBy('department').agg(max('salary')).show()

#Count number of employees in each citydf.
df.groupBy('city').count().show()
#--from pyspark.sql.functions import count

#df.groupBy("city").agg(count("ename")).show()

#Find total salary per city
df.groupBy('city').agg(sum('salary').alias("total_salary")).show()

#Find employees whose salary is between 40000 and 60000
df.filter((df.salary<60000)&(df.salary>40000)).select('*').show()

#Find employees whose name starts with 'A'
#df.select('name').startswith('A').show()w rong
df.filter(df.name.startswith('A')).select('name').show()

#Find employees whose city is either Bangalore or Pune
df.filter((df.city=='Bangalore')| (df.city=='Pune')).select('name').show()

#Find employees whose salary is NOT between 40000 and 60000
df.filter((df.salary > 60000) | (df.salary < 40000)) \
.select("name") \
.show()
df.printSchema()

#Find employees whose name contains letter 'a' (case sensitive)
df.filter(df.name.endswith('a')).show()

#Find employees whose name length is greater than 5
df.filter(length('name')>5).select('*').show()
df.filter(length(col("name")) > 5).select("name").show()

#Find employees whose salary is divisible by 2
df.filter(col('salary')%2==0).select('name').show()