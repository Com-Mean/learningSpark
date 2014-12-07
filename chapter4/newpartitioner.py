#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: newpartitioner.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: 2014年11月23日 星期日 18时57分37秒
#########################################################################
from pyspark import SparkConf,SparkContext,StoreLevel
import urlparse

# use the domain as the hash src string
def hash_domain(url):
    return hash(urlparse.urlparse(url).netloc)

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)
rdd = sc.parallelize([1,2,3,4])
rdd.partitonBy(20, hash_domain)  # Create 20 patitions
