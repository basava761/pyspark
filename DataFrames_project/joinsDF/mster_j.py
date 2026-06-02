from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('joins_practice').getOrCreate()

employees = [
    (1, "Alice", "HR"),
    (2, "Bob", "IT"),
    (3, "Charlie", "Finance"),
    (4, "David", "IT"),
    (5, "Eva", "HR")
]
emp_df = spark.createDataFrame(employees, ["EmpID", "Name", "Dept"])
emp_df.show()

departments = [
    ("HR", "Human Resources"),
    ("IT", "Information Technology"),
    ("Finance", "Financial Dept"),
    ("Sales", "Sales Dept")
]
dept_df = spark.createDataFrame(departments, ["DeptCode", "DeptName"])
dept_df.show()

print(' Write a query to list Employee Name and Department Name using an INNER JOIN.')

#select Name,department from employees e inner join  department d on e.dept=d.deptcode;

emp_df.alias("e").join(dept_df.alias("d"), col("e.Dept") == col("d.DeptCode"), "inner").select(col("e.Name"), col("d.DeptName")) \
    .show()

#2. Write a query to list all employees and their department names, but also include employees whose department doesn’t exist in the departments table.
emp_df.alias('e').join(dept_df.alias('d'),col('e.Dept')==col('d.DeptCode'),'left').show()

#3.Write a query to list all departments and their employees, but also include departments that don’t have any employees.

emp_df.alias('e').join(dept_df.alias('d'),col('e.Dept')==col('d.DeptCode'),'right').select(col('e.Name'),col('d.DeptName')).show()

#4.List all employees and all departments, showing matches where they exist, and null where they don’t.
emp_df.alias('e').join(dept_df.alias('d'),col('e.Dept')==col('d.DeptCode'),'full').select(col('e.Name').alias('Employee_Name'),col('d.DeptName').alias('DepartmentName')).show()
emp_df.alias('e').join(dept_df.alias('d'),col('e.Dept')==col('d.DeptCode'),'outer').select(col('e.Name').alias('Employee_Name'),col('d.DeptName').alias('DepartmentName')).show()


#5.Write a query to list every possible combination of Employee and Department
emp_df.alias('e').join(dept_df.alias('d'),how='cross').show()

from pyspark.sql.functions import col

emp_df.alias("e").join(dept_df.alias("d"), how="cross").select(col("e.Name").alias("Employee_Name"),col("d.DeptName").alias("DepartmentName")).show()

#Seif join
# List pairs of employees who work in the same department.
#SELECT e1.Name AS Employee1, e2.Name AS Employee2, e1.Dept FROM employees e1 JOIN employees e2 ON e1.Dept = e2.DeptWHERE e1.EmpID < e2.EmpID;
emp_df.alias('e').join(emp_df.alias('e2'),col('e.Dept')==col('e2.Dept'),'inner').filter(col('e.EmpID')<col('e2.EmpID')).show()
