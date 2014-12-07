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
