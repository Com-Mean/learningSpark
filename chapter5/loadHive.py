#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: loadHive.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: 2014年11月30日 星期日 21时31分06秒
#########################################################################


from pyspark import SparkConf,SparkContext,StoreLevel
from pyspark.sql import HiveContext, SQLContext


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)

hiveCtx = HiveContext(sc)
rows = hiveCtx.hql("SELECT key,value FROM src")
keys = rows.map(lambda row: row["key"])

# Parquet load example
sqlCtx = SQLContext(sc)
rows = sqlCtx.ParquetFile("people.parquet")
names = rows.map(lambda row:row["name"])
