#from pyspark.sql import SparkSession
#import numpy as np

#spark=SparkSession.builder.appName('test').getOrCreate()
#sc=spark.sparkContext
#p="hdfs://localhost:9000/grouping/data.txt"
#d=sc.textFile(p)
#r=d.collect()
#print(r) #['Alice,HR,3000', 'Bob,IT,4000', 'Charlie,HR,3500', 'David,Finance,5000', 'Eve,IT,4200', 'Frank,Finance,5500', 'Grace,HR,2800', 'Heidi,IT,3900', 'Ivan,Finance,6000', 'Judy,HR,3100', '']

#r1=d.map(lambda l:l.split(','))
#print(r1,'****************************************************************')
#d_sal=r1.map(lambda f:(f[1],int(f[2])))
#dep_c=d_sal.mapValues(lambda x:1).reduceByKey(lambda a,b:a+b)
#d_t=d_sal.reduceByKey(lambda a,b:a+b)

#print(d_sal.collect(),'deptment salary')
#print(d_t.collect(),'department total')
#---------------------------------------------------------------------------------------------
from pyspark import SparkContext

sc = SparkContext("local", "GroupingAggregation")
p="hdfs://localhost:9000/grouping/data.txt"

data = sc.textFile(p)

# Split and filter out malformed lines
records = data.map(lambda line: line.split(",")) \
        .filter(lambda fields: len(fields) == 3)

# Map to (Department, Salary)
dept_salary = records.map(lambda fields: (fields[1].strip(), int(fields[2].strip())))

# Count employees per department
dept_counts = dept_salary.mapValues(lambda x: 1).reduceByKey(lambda a, b: a + b)

# Total salary per department
dept_totals = dept_salary.reduceByKey(lambda a, b: a + b)

# Average salary per department
dept_avg = dept_totals.join(dept_counts).mapValues(lambda x: x[0] / x[1])

print("Counts:", dept_counts.collect()) #Counts: [('HR', 4), ('IT', 3), ('Finance', 3)]

print("Totals:", dept_totals.collect()) #Totals: [('HR', 12400), ('IT', 12100), ('Finance', 16500)]

print("Averages:", dept_avg.collect()) #Averages: [('HR', 3100.0), ('IT', 4033.3333333333335), ('Finance', 5500.0)]

