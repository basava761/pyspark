from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('test').getOrCreate()


employees_data = [
    (1,'John',101,50000),
    (2,'Ravi',102,60000),
    (3,'Sam',103,55000),
    (4,'David',104,70000),
    (5,'Mike',None,45000),
    (6,'Asha',105,65000),
    (7,'Kiran',101,52000),
    (8,'Neha',106,48000)
]

employees_cols = [
    "emp_id",
    "emp_name",
    "dept_id",
    "salary"
]

emp_df = spark.createDataFrame(
    employees_data,
    employees_cols
)
departments_data = [
    (101,'IT'),
    (102,'HR'),
    (103,'Finance'),
    (104,'Sales'),
    (107,'Marketing')
]

departments_cols = [
    "dept_id",
    "dept_name"
]

dept_df = spark.createDataFrame(
    departments_data,
    departments_cols
)
emp_df.show()
print(emp_df.columns)
dept_df.show()
print(dept_df.columns)

#Display all employees along with their department names.
#1 select e.emp_name,e.emp_id,d.dept_id from employees e inner join departments d on e.dept_id=d.dept_id;
emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'inner').show()

#2.Display all employees and their department names.

#Even if a department does not exist, the employee must still appear.

#select e.emp_name,d.dept_name from employees e left join departments d on e.dept_id=d.dept_id;
emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'left').select(col('e.emp_name'),col('d.dept_name')).show()

#Find employees whose department does NOT exist in the departments table.

#select emp_name from employees e left join departments d on e.dept_id=d.dept_id where d.dept_id is null;
emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'left_anti').select(col('e.emp_name')).show()

#Find employees whose department exists in the departments table.

#select * from employees e left join departments d on e.dept_id=d.dept_id where d.dept_id is null;

emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'left_anti').show()

emp_df.alias('e') \
.join(
    dept_df.alias('d'),
    col('e.dept_id') == col('d.dept_id'),
    'left_anti'
) \
.show()

emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'outer').select(col('e.emp_name'),col('d.dept_name')).show()