from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('count').getOrCreate()

sc=spark.sparkContext

path="hdfs://localhost:9000/test/partitions/demo.txt"

#r1=sc.textFile(path)
#words = r1.flatMap(lambda line: line.split()) 
#pairs = words.map(lambda word: (word, 1))  
#r=r1.collect()
#print(r) #["Hello-world 'hadoop'"]
#w=map(lambda x:x,count(x),r)
#print(w)
r1 = sc.textFile(path)
words = r1.flatMap(lambda line: line.split())   # split each line into words
pairs = words.map(lambda word: (word, 1))       # (word, 1)
counts = pairs.reduceByKey(lambda a, b: a + b)  # sum counts per word
print(counts.collect())
