from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('win').getOrCreate()

data = [
(101,'John','IT',60000,'Bangalore','2021-01-15'),
(102,'Mike','IT',75000,'Hyderabad','2020-03-10'),
(103,'Sara','IT',75000,'Chennai','2022-07-12'),
(104,'David','HR',45000,'Mumbai','2019-08-20'),
(105,'Emma','HR',55000,'Delhi','2021-11-01'),
(106,'James','Finance',90000,'Pune','2018-05-18'),
(107,'Sophia','Finance',85000,'Bangalore','2020-09-25'),
(108,'Liam','Finance',85000,'Hyderabad','2022-02-14'),
(109,'Olivia','IT',50000,'Chennai','2023-04-01'),
(110,'Noah','HR',45000,'Mumbai','2022-06-30')
]

cols = ['emp_id','emp_name','department','salary','city','joining_date']

df = spark.createDataFrame(data, cols)

df.show()

#+------+--------+----------+------+---------+------------+
#|emp_id|emp_name|department|salary|     city|joining_date|
#+------+--------+----------+------+---------+------------+
#|   101|    John|        IT| 60000|Bangalore|  2021-01-15|
#|   102|    Mike|        IT| 75000|Hyderabad|  2020-03-10|
#|   103|    Sara|        IT| 75000|  Chennai|  2022-07-12|
#|   104|   David|        HR| 45000|   Mumbai|  2019-08-20|
#|   105|    Emma|        HR| 55000|    Delhi|  2021-11-01|
#|   106|   James|   Finance| 90000|     Pune|  2018-05-18|
#|   107|  Sophia|   Finance| 85000|Bangalore|  2020-09-25|
#|   108|    Liam|   Finance| 85000|Hyderabad|  2022-02-14|
#|   109|  Olivia|        IT| 50000|  Chennai|  2023-04-01|
#|   110|    Noah|        HR| 45000|   Mumbai|  2022-06-30|
#+------+--------+----------+------+---------+------------+

#Assign a unique sequence number to employees based on highest salary first.
#select * ,row_number() over(order by salary desc) as sal from employees;
w=Window.orderBy(col('salary').desc())
df.withColumn('salary',row_number().over(w)).show()

#Get rank of employees based on salary.
#If two employees have the same salary, they should get the same rank.

# select emp_name, salary,Rank() over(order By salary ) as rn from employees;
w=Window.orderBy(col('salary'))
df.withColumn('rnk',rank().over(w)).select(col('emp_name'),col('salary'),'rnk').show()

#Get the second highest salary employee(s).
#select salary,Dense_Rank() over(order by salary desc)as rn from employees where rn=2;

#select salary from(select salary,Dense_Rank() over(order by salary desc)as rn from employees) t where rn=2 ;
w=Window.orderBy(col('salary').desc())
df.withColumn('rn',dense_rank().over(w)).filter(col('rn')==2).select('rn','salary').show()

#Get the highest paid employee in EACH department.
#select emp_name,department, salary from(select *,Rank()over(partition by department order by salary desc) as rn from employees) t
# where rn=1;
w=Window.partitionBy('department').orderBy(col('salary').desc())
df.withColumn('rn',rank().over(w)).select('department','emp_name','salary',).filter(col('rn')==1).show()

#Get the previous employee salary within each department.

#select emp_name,salary ,lag('salary') over(partition By department order by salary) as previous from employees;
w=Window.partitionBy('department').orderBy('salary')
df.withColumn('previous_sal',lag('salary').over(w)).explain(True)


