#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: actions.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: Fri Nov 14 10:03:50 2014
#########################################################################
from pyspark import SparkConf,SparkContext,StoreLevel
from operator import add,sub


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)
nums = sc.parallelize([1,2,3,4])
nums.reduce(lambda x,y: x+y)  # 1 + 2 + 3 + 4
nums.fold(0, add)             # 1 + 2 + 3 + 4
nums.fold(0, lambda x,y: x+y) # 1 + 2 + 3 + 4
nums.fold(1, lambda x,y: x*y) # 1 + 2 + 3 + 4
seqOp = (lambda x,y:(x[0]+y,x[1]+1))
combOp = (lambda x,y:(x[0]+y[0], x[1]+y[1]))
sumCount=nums.aggregate((0,0), seqOp, combOp) # map then combine
avg =sumCount[0] / float(sumCount[1]) # sum/count = 10/4

# basic actions
nums = sc.parallelize([1,2,3,3])
nums.collect()                  # [1,2,3,3]
nums.count()                    # 4
nums.take(2)                    # [1,2]
nums.top(2)                     # [3,3]
nums.takeOrdered(2)             # [1,2]
nums.takeOrdered(2, key=lambda x: -x)              # [3,3]
nums.takeSample(False,1)  # non-determin
nums.reduce(add) # == nums.fold(0, add)
nums.aggregate((0,0), seqOp, combOp)
def foo(x): print(x)
nums.foreach(foo) # Apply the provided function to each element of the RDD

# Converting between RDD Types
# cache, tachyon will be better
nums.persist(StoreLevel.MEMORY_ONLY)
nums.unpersist()

# key-value pairs actions
sumCount=nums.mapValues(lambda x:(x, 1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

# result = sc.textFile("s://...").flatMap(lambda x: x.split(" ")).countByValue()
input = sc.textFile("s://...")
words = input.flatMap(lambda x:x.split(" "))
result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

# reduceByKey with custom parallelism
data = [("a",3), ("b",4), ("a",1)]
sc.parallelize(data).reduceByKey(lambda x,y:x+y)
sc.parallelize(data).reduceByKey(lambda x,y:x+y, 10) # custom parallelism with level

# sort the rdd by key
rdd.sortByKey(ascending=True, numPartitions=None, keyfunc=lambda x:str(x))

# transformations on one Pair RDD(example[(1,2),(3,4),(3,6)])
rdd = sc.parallelize([(1,2),(3,4),(3,6)])
rdd.combineByKey() #TODO
rdd.groupByKey() # [(1,[2]),(3,[4,6])]
rdd.reduceByKey(lambda x,y:x+y) # [(1,2),(3,10)]
rdd.mapValues(lambda x:x+1) # [(1,3),(3,5),(3,7)]
rdd.flatMapValues(lambda x:range(x,6)) # [(1,2),(1,3),(1,4),(1,5),(3,4),(3,5),(3,6)]
rdd.keys()                  # (1,3,3)
rdd.values()                # (2,4,6)
rdd.sortBykey()             # [(1,2),(3,4),(3,6)]

# Transformations on two Pair RDD [(1,2),(3,4),(3,6)] [(3,9)]
other = sc.parallelize([(3,9)])
rdd.subtractByKey(other)        # [(1,2)]
rdd.join(other)                 # [(3,(4,9)),(3,(6,9))]
rdd.rightOuterJoin(other)       # [(3,(Some(4),9)),(3,(Some(6),9))]
rdd.leftOuterJoin(other)        # [(1,(2,None)),(3,(4,Some(9))),(3,(6,Some(9))]
rdd.cogroup()                   # [(1,([2],[])),(3,([4,6],[9]))]

# Actions on two Pair RDD
rdd.countByKey()                # [(1,1),(3,2)]
rdd.collectAsMap()              # Map([(1,2),(3,4),(3,6)])
rdd.Lookup(3)                   # [4,6]
