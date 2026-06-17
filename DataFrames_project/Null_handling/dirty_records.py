from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('dirty').getOrCreate()




data = [
(1,'John','IT',70000,'Bangalore'),
(2,'Mary','HR',None,'Hyderabad'),
(3,'David',None,90000,'Bangalore'),
(4,'Alice','Finance',80000,None),
(5,'Tom','IT',120000,'Mumbai'),
(5,'Tom','IT',120000,'Mumbai'),
(6,'Emma','HR',100000,'Hyderabad'),
(7,'Sophia','Finance',110000,'Chennai'),
(8,'Chris','IT',75000,'Pune'),
(9,'Neha','Finance',None,'Bangalore'),
(10,'Arjun','HR',65000,'Unknown')
]

cols = [
'emp_id',
'emp_name',
'department',
'salary',
'city'
]

emp_df = spark.createDataFrame(data, cols)

emp_df.show()
emp_df.explain()
print('----------------------------------------------------------------------------------')
emp_df.explain(True)
#Find all employees whose salary is NULL.
#select * from demp where salary is null;

emp_df.filter(col('salary').isNull()).select('emp_id','emp_name').show()
#emp_df.filter(col('salary').isNull()).select('emp_id','emp_name').explain(True)

#Replace NULL salaries with 0.
#select emp_id,emp_name ,IFNull(salary,0) from demp;


emp_df.withColumn("salary",when(col("salary").isNull(),0).otherwise(col("salary"))).show()
emp_df.withColumn('salary',when(col('salary').isNull(),0).otherwise(col('salary'))).show()

#Replace NULL department values with "Unknown".

#select emp_id,emp_name ,IFNull(department,'unknown') from demp;
emp_df.withColumn('department',when(col('department').isNull(),'unknown').otherwise(col('department'))).show()

#Find all duplicate employees based on emp_id

#select emp_id ,count(*) from demp group by emp_id having count(*)>1;


