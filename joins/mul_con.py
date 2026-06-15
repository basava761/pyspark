from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder.appName("JoinsPractice").getOrCreate()

# Employees DataFrame
employees_data = [
    (1, "Ravi", 10, 50000, "ravi@abc.com"),
    (2, "Anita", 20, None, "anita@abc.com"),   # salary NULL
    (3, "Ravi", 10, 50000, "ravi@abc.com"),    # duplicate
    (4, "Kiran", None, 40000, None)            # dept_id NULL, email NULL
]

emp_df = spark.createDataFrame(employees_data, 
    ["emp_id", "name", "dept_id", "salary", "email"]
)

# Departments DataFrame
departments_data = [
    (10, "HR", "Hyderabad"),
    (20, "Finance", "Mumbai"),
    (30, "IT", "Delhi")
]

dept_df = spark.createDataFrame(departments_data, 
    ["dept_id", "dept_name", "location"]
)
emp_df.show()
dept_df.show()

#We want to find all employees who don’t belong to any department (i.e., dept_id is missing in the departments table).
#mysql> select e.name,d.dept_name from emp e left join dept d on e.dept_id=d.dept_id where e.dept_id is null;
emp_df.alias('e').join(dept_df.alias('d'),col('e.dept_id')==col('d.dept_id'),'left').filter(col('e.dept_id'). isNull()).show()

#Buddy, shall we move to Scenario 2 now: 
# “Find employees who share the same salary but belong to different departments” 

#SELECT e1.emp_id, e1.name, e1.salary, e1.dept_id FROM emp e1 JOIN emp e2 ON e1.salary = e2.salary AND e1.dept_id <> e2.dept_id;

#SELECT e1.name, e1.email, COUNT(*) AS dup_count
#FROM emp e1
#GROUP BY e1.name, e1.email
#HAVING COUNT(*) > 1;



