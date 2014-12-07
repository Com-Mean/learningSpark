#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: createRDD.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: Fri Nov 14 08:36:38 2014
#########################################################################
from pyspark import SparkConf,SparkContext


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)
#Create RDD by loading external dataset
lines = sc.textFile("README.md")
#Create RDD by parallelize the collection in a driver program
lines = sc.parallelize(["pandas", "i like pandas"])

