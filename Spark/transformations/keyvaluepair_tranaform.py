from pyspark.sql import SparkSession
import numpy as np

spark=SparkSession.builder.appName('test').getOrCreate()

sc=spark.sparkContext

"""
transformation on a pair rdd's i,e (k,v) pairs
1.reduceByKey()
2.groupBYKey()
3.sortByKey()
4.mapValues()
5.keys()
6.values()
7.join()
8.leftOuterJoin()
9.rightOuterJoin()
10.fullOuterjoin()

"""
#reducebyKey():sum up all the values with same keyreduceByKey can be applied on (k,v) pairs
#============================================================================================
data = [("apple", 2), ("banana", 3), ("apple", 4), ("banana", 1), ("orange", 5)]

r1=sc.parallelize(data)
rdd=r1.reduceByKey(lambda x,y:x+y).collect()
print(rdd) # [('apple', 6), ('banana', 4), ('orange', 5)]


#===============================================================
r2=sc.parallelize([(11,100000),(12,20000),(13,30000),(11,2000),(12,24000)])

res=r2.reduceByKey(lambda a,b:a+b).collect()
print(res) #[(12, 44000), (13, 30000), (11, 102000)]


#=========================================================================
#2. group BYKey()
r=sc.parallelize([(11,100000),(12,20000),(13,30003),(11,2000),(12,24000)])

r3=r.groupByKey().mapValues(list).collect()
print(r3,'groupbyKey===========================================>>>')# [(12, [20000, 24000]), (13, [30000]), (11, [100000, 2000])]

#====================================sortBYKey================================================

t=r.sortByKey().collect()
print(t) #[(11, 100000), (11, 2000), (12, 20000), (12, 24000), (13, 30000)]
#=======================================MapValues============================================

m=r.mapValues(lambda x:x%2).collect()
print(m,'======mapvaluemethod==========>') #even[(11, 0), (12, 0), (13, 0), (11, 0), (12, 0)] ======mapvaluemethod== ========>

#odd [(11, 0), (12, 0), (13, 1), (11, 0), (12, 0)]

#============================joins=======================================
a = sc.parallelize([1,2,3,4,5,6]).map(lambda x: (x, 1))
b = sc.parallelize([1,2,3,7,8,9]).map(lambda x: (x, 1))

rdd = a.join(b).collect()
print(rdd)


#inner join: only matchings

rdd=a.join(b).collect()
print(rdd) #[(1, (1, 1)), (2, (1, 1)), (3, (1, 1))]

lj=a.leftOuterJoin(b).collect()
rj=a.rightOuterJoin(b).collect()
#print(lj,'====================================================<<<<<<<<<<<<<<<<<<<<<<,')(1, (1, 1)), (2, (1, 1)), (3, (1, 1)), (4, (1, None)), (5, (1, None)), (6, (1, None))]
#print(rj,'====================================================<<<<<<<<<<<<<<<<<<<<<<,')[(8, (None, 1)), (1, (1, 1)), (9, (None, 1)), (2, (1, 1)), (3, (1, 1)), (7, (None, 1))]
