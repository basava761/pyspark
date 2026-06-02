from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('prac').getOrCreate()
from pyspark.sql.functions import *




data = [
    (101, "John", "IT", 60000, 201, "john@gmail.com"),
    (102, "Alice", "HR", 50000, 202, "alice@gmail.com"),
    (103, "Bob", "IT", None, 201, "bob@gmail.com"),
    (104, "David", None, 55000, 203, "david@gmail.com"),
    (105, "Eva", "Finance", 70000, None, "eva@gmail.com"),
    (106, "Mike", "IT", 60000, 201, None),

    # Exact duplicates
    (101, "John", "IT", 60000, 201, "john@gmail.com"),
    (102, "Alice", "HR", 50000, 202, "alice@gmail.com"),

    # Partial duplicates
    (107, "John", "IT", 60000, 201, "john_new@gmail.com"),
    (108, "Alice", "HR", 50000, 202, "alice_new@gmail.com"),

    # More NULL cases
    (109, None, "Sales", 45000, 204, "sales1@gmail.com"),
    (110, "Chris", "Sales", None, None, None),
    (111, None, None, None, None, None),

    # Duplicate email
    (112, "Tom", "IT", 65000, 201, "john@gmail.com")
]

columns = [
    "emp_id",
    "emp_name",
    "department",
    "salary",
    "manager_id",
    "email"
]

df = spark.createDataFrame(data, columns)

#df.show(truncate=False)
df.show()
df.persist()
#df.printSchema()
#print(df.columns)

#Display all employees whose salary is NULL.
#mysql> select emp_name from employees where salary is Null;
df.select('emp_name').filter(col('salary').isNull()).show()

#Display employees whose department is NOT NULL.
#select emp_name from employees where department is not null;
df.select('emp_name').filter(col('department').isNotNull()).show()

#"Find employees for whom both salary information and manager assignment are missing. We need to send these records to HR for cleanup."
#select emp_name,salary,manager_id from employees where salary is null and manager_id is null;
df.select('emp_name','salary','manager_id').filter((col('salary').isNull()) & (col('manager_id').isNull())).show()

#print(col)
#print(type(col))
#print(type(col('salary')))
#print(type(col('salary').isNull))

#print(
#    (col('salary').isNull()) &
#    (col('manager_id').isNull())
#)

#condition = (
#    (col('salary').isNull()) &
#    (col('manager_id').isNull())
#)

#df.filter(condition).show()

#We have some incomplete employee records. Find all employees where either employee name OR department is missing."
#select * from employees where emp_name is null or department is null;
cond=((col('emp_name').isNull())|(col('department').isNull()))
df.filter(cond).select('*').show()


#Find employees whose email is available but manager information is missing
#select * from employees where email is not null and manager_id is null;

con=(col('email').isNotNull())&(col('manager_id').isNull())

df.filter(con).select('*').show()

#Data Governance Team says:

#"Identify employees where all of these fields are missing:

#emp_name
#department
#salary
#manager_id
#email

#These are completely blank employee records that should be quarantined."

#select * from employees where (emp_name and department and salary and manager_id) is null;
con=((col('emp_name').isNull())&(col('salary').isNull())&(col('department').isNull())&(col('manager_id').isNull()))
df.select('*').filter(con).show()

#Find employees whose salary exists but email is missing.
#select * from employees where salary is not null and email is null;
c=((col('salary').isNotNull())&(col('email').isNull()))
df.select('*').filter(c).show()

#Find employees whose department information is available, but either salary or email is missing.
#select * from employees where (department is not null and (salary|email) is null);
c=(
    (col('department').isNotNull()) &
    ((col('salary').isNull())|(col('email').isNull()))
)
df.select('*').filter(c).show()


#Find employees where at least one of the following is missing:

#emp_name
#department
#salary
#manager_id
#email
#select * from employees where emp_name is null or department is null or salary is null or manager_id is null or email is null;
c=((col('emp_name').isNull())|(col('department').isNull())|(col('salary').isNull())|(col('manager_id').isNull())|(col('email').isNull()))
df.filter(c).agg(count('*').alias('total_count')).show()
df.filter(c).show()

#Count how many employees have a NULL salary.

#select count(*) from employees where salary is null;
df.filter(col('salary').isNull()).agg(count('*')).show()


##Find the number of NULL values present in each column."

##mysql> select count(emp_name),count('department'),count('salary') from employees where (salary or emp_name or department or email) is null;
#df.filter(col('emp_name').agg(count('*')),col('salary').agg(count('*')),col('department').agg(count('*')),col('email').agg(count('*'))).show()

from pyspark.sql.functions import *

df.agg(
    sum(when(col("emp_name").isNull(), 1).otherwise(0)).alias("emp_name_null_count"),
    sum(when(col("department").isNull(), 1).otherwise(0)).alias("department_null_count"),
    sum(when(col("salary").isNull(), 1).otherwise(0)).alias("salary_null_count"),
    sum(when(col("manager_id").isNull(), 1).otherwise(0)).alias("manager_id_null_count"),
    sum(when(col("email").isNull(), 1).otherwise(0)).alias("email_null_count")
).show()

#select
#    count(*) - count(emp_name) as emp_name_null_count,
#    count(*) - count(department) as department_null_count,
#    count(*) - count(salary) as salary_null_count,
#    count(*) - count(manager_id) as manager_id_null_count,
#    count(*) - count(email) as email_null_count
#from employees;

#Whenever salary is missing, replace it with 0
#df.select('*').when(col('salary').isNull(),1).show()

#df.withColumn("salary",when(col("salary").isNull(), 0).otherwise(col("salary"))).show()
#mysql> select
#    ->     emp_id,
#    ->     emp_name,
#    ->     department,
#    ->     coalesce(salary, 0) as salary,
#    ->     manager_id,
#    ->     email
#    -> from employees;
