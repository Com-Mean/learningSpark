#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: initSpark.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: Fri Nov 14 01:02:19 2014
#########################################################################
# Initializing Spark in Python
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)
