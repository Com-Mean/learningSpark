#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: transformations.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: Fri Nov 14 09:18:13 2014
#########################################################################
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)
nums = sc.parallelize([1, 2, 3, 4])

# map, single output of each element, return iterator of RDD
squared = nums.map(lambda x:x * x).collect()
print(squared)

# flatMap, multiple output of each element, return RDD, not iterators
lines = sc.parallelize(["hello world", "hi"])
words = lines.flatMap(lambda line: line.split(" "))
words.first() # return hello

# Basic RDD transformations
rdd = sc.parallelize([1, 2, 3, 3])
rdd.map(lambda x: x + 1).collect() #[2, 3, 4, 4]
rdd.flatMap(lambda x: range(x, 4)) #[1,2,3, 2,3, 3, 3]
rdd.filter(lambda x: x != 1)       # [2, 3, 3]
rdd.distinct() # [1, 2, 3]
rdd.sample(False, 0.5) # sample an Rdd

# Two-RDD transformations
la = sc.parallelize([1,2,3]); lb = sc.parallelize([3, 4, 5])
la.union(lb) # [1,2,3,3,4,5]
la.intersection(lb) # [3]
la.subtract(lb) # [1,2]
la.cartesian(lb) # [(1,3), (1,4), (1,5), (2,3), (2, 4), (2, 5), (3, 3), (3, 4), (3, 5)]
