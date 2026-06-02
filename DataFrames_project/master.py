from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("PracticeDF").getOrCreate()

# =========================
# EMPLOYEE DATAFRAME
# =========================

employee_data = [
    (1, "Basava", "IT", 50000, 28, "Bangalore"),
    (2, "Ravi", "HR", 40000, 30, "Hyderabad"),
    (3, "Ajay", "IT", 60000, 26, "Chennai"),
    (4, "Kiran", "Finance", 45000, 35, "Mumbai"),
    (5, "Neha", "IT", 70000, 29, "Bangalore"),
    (6, "Priya", "HR", 38000, 32, "Delhi"),
    (7, "Arun", "Finance", 52000, 31, "Pune"),
    (8, "John", "IT", 80000, 27, "Chennai"),
    (9, "David", "Sales", 30000, 25, "Hyderabad"),
    (10, "Meena", "Sales", 35000, 24, "Mumbai"),
    (10, "Meena", "Sales", 35000, 24, "Mumbai")  # duplicate
]

employee_columns = [
    "emp_id",
    "name",
    "department",
    "salary",
    "age",
    "city"
]

emp_df = spark.createDataFrame(employee_data, employee_columns)

# =========================
# DEPARTMENT DATAFRAME
# =========================

department_data = [
    ("IT", "Technology"),
    ("HR", "Human Resource"),
    ("Finance", "Accounts"),
    ("Sales", "Marketing")
]

department_columns = [
    "department",
    "dept_full_name"
]

dept_df = spark.createDataFrame(department_data, department_columns)

# =========================
# NULL DATAFRAME
# =========================

null_data = [
    (11, None, "IT", None, 30, "Bangalore"),
    (12, "Rakesh", None, 45000, None, "Pune")
]

null_df = spark.createDataFrame(null_data, employee_columns)

# =========================
# EXTRA DATAFRAME FOR UNION
# =========================

extra_data = [
    (13, "Rahul", "IT", 65000, 29, "Delhi"),
    (14, "Sneha", "HR", 42000, 31, "Mumbai")
]

extra_df = spark.createDataFrame(extra_data, employee_columns)

# Preview
#emp_df.show()
#dept_df.show()
#null_df.show()
#extra_df.show()

##Show only employee name and salary.
##q=select employee_name,salary;
#emp_df.select("name",'salary',).show()

##Filter employees whose salary is greater than 50000.
##select employeee from employees where salary <50,000
#emp_df.filter(col('salary')>50000).select('name','salary').show()


##SELECT department,AVG(salary)FROM employees GROUP BY department;
#emp_df.groupBy(col('department')).agg(avg(col('salary'))).show()

##SELECT department,COUNT(*) AS total_employees FROM employees GROUP BY department;
##emp_df.groupBy('department').count().alias('total_employees').show()-->aias wont apply here
#emp_df.groupBy('department').agg(count('*').alias('total_employees')).show()

##SELECT * FROM employees ORDER BY salary DESC;
#emp_df.select('*').orderBy(col('salary').desc()).show()

##SELECT department,MAX(salary) AS max_salary FROM employees GROUP BY department;
#emp_df.groupBy('department').agg(max(col('Salary')).alias('maximum_salary')).show()

##SELECT department,SUM(salary) AS total_salary,AVG(salary) AS avg_salary FROM employees GROUP BY department;
#emp_df.groupBy('department').agg(sum(col('salary')).alias('total_salary'),avg(col('salary')).alias('average_salary')).show()

##SELECT * FROM employees WHERE department = 'IT' AND salary > 60000;
##emp_df.select('*').filter((col('department')=='IT)'&(col('salary')>60000))).show()
#emp_df.select('*').filter((col('department')=='IT')&(col('salary')>60000)).show()

##SELECT * FROM employees WHERE department = 'IT' OR department = 'HR';
##emp_df.select('*').filter((col('department')=='IT')|(col('department')=="HR")).show()

#emp_df.select('*').filter(col('department').isin('HR',"IT")).show()

#SELECT name,salary,salary * 0.20 AS bonus FROM employees;
#emp_df.select('Name','salary').withColumn('bonus',col('salary')*0.20).show()
#emp_df.select('name','salary',col('salary')*0.20).alias('bonus').show()

##SELECT DISTINCT department FROM employees;

#emp_df.select('department').distinct().show()

##SELECT * FROM employees LIMIT 5;
#emp_df.limit(5).show()


#emp_df.select('*').limit(5).show()

#SELECT * FROM employees ORDER BY salary ASC;
#emp_df.orderBy(col('salary').asc()).show()

emp_df.orderBy('salary').show()

#SELECT department,COUNT(*) AS employee_count,AVG(salary) AS avg_salary FROM employees GROUP BY department HAVING COUNT(*) > 2;
emp_df.groupBy('department').agg(count('*').alias('employee_count'),avg(col('salary')).alias('average_salary')).filter(col('employee_count')>2).show()

#SELECT e.name,e.department, d.dept_full_name FROM employees e
#INNER JOIN departments d ON e.department = d.department;


emp_df.join(dept_df,emp_df.department == dept_df.department,"inner").show()
emp_df.alias("e") \
.join(
    dept_df.alias("d"),
    col("e.department") == col("d.department"),
    "inner"
) \
.select(
    col("e.name"),
    col("e.department"),
    col("d.dept_full_name")
) \
.show()


#Updated memory

#Got it. I've saved that.

#When you come back, you can simply say:

#"Continue PySpark DataFrame practice"

#and I'll resume from this point: