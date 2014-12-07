#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: opreatingJson.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: 2014年11月29日 星期六 21时33分40秒
#########################################################################

import json
from pyspark import SparkConf,SparkContext,StoreLevel


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)

# read json
input = sc.textFile("file:///home/JSON_FILE_PATH")
data = input.map(lambda x:json.loads(x))

# save as json
(data.filter(lambda x:x['LovesPandas'])).map(lambda x:json.dumps(x)).saveAsTextFile("output_file_path")
