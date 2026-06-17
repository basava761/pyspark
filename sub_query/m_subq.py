from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('query').getOrCreate()




employee_data = [
    (1,'John','IT',70000,5,'Bangalore','2023-01-15'),
    (2,'Mary','HR',60000,6,'Hyderabad','2023-03-20'),
    (3,'David','IT',90000,5,'Bangalore','2024-02-10'),
    (4,'Alice','Finance',80000,7,'Chennai','2024-05-12'),
    (5,'Tom','IT',120000,None,'Mumbai','2022-07-01'),
    (6,'Emma','HR',100000,None,'Hyderabad','2022-06-15'),
    (7,'Sophia','Finance',110000,None,'Chennai','2022-08-21'),
    (8,'Chris','IT',75000,5,'Pune','2025-01-05'),
    (9,'Neha','Finance',95000,7,'Bangalore','2025-02-14'),
    (10,'Arjun','HR',65000,6,'Pune','2025-03-18')
]

employee_cols = [
    "emp_id",
    "emp_name",
    "department",
    "salary",
    "manager_id",
    "city",
    "joining_date"
]

emp_df= spark.createDataFrame(
    employee_data,
    employee_cols
)

emp_df.show()
#emp_df.explain(True)

print(emp_df.rdd.getNumPartitions())

#Find all employees whose salary is greater than the average salary of the company.

#mysql> select emp_name ,salary from employees where salary >(select avg(salary)
#from employees);
avg_sal = emp_df.agg(
    avg("salary")
).collect()[0][0]

emp_df.filter(
    col("salary") > avg_sal
).select(
    "emp_id",
    "emp_name",
    "salary",
    "department"
).show()


avg_sal = emp_df.agg(avg("salary")).collect()
avg_sal = emp_df.agg(avg("salary")).collect()[0]
avg_sal = emp_df.agg(avg("salary")).collect()[0][0]

#Find all employees whose salary is greater than Tom's salary.
#mysql> select emp_name,salary from employees where salary>(select salary from employees where emp_name='john');
print('printing johns salary')
sal = emp_df.filter(
    col("emp_name") == "John"
).select("salary").first()[0]

emp_df.select(
    "emp_name",
    "salary"
).filter(
    col("salary") > sal
).show()