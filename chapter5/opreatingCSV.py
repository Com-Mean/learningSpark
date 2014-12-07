#!/usr/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: opreatingCSV.py
# Author: lpqiu
# mail: qlp_1018@126.com
# Created Time: 2014年11月29日 星期六 21时50分36秒
#########################################################################

import csv
import StringIO

from pyspark import SparkConf,SparkContext,StoreLevel


conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf)

# not contain newlines in any of the fileds in CSVfile
def loadRecord(line):
    """Parse a CSV line"""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, filednames=["name", "favouriteAnimal"])
    return reader.next()

data = sc.textFile("file:///home/FILE_PATH").map(loadRecord)


# contain newlines in fileds, load each file full and then parse the entire segment
def loadRecords(fileNameContents):
    """Load all the records in a given file"""
    input = StringIO.StringIO(fileNameContents[1])
    reader = csv.DictReader(input, filednames=["name", "favouriteAnimal"])
    return reader

fullFileData = sc.wholeTextFiles("input_files_dir").flatMap(loadRecords)

# tansport RDD to cvs file format, Save as CSV file
def writeRecords(records):
    """"Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, filednames=["name", "favouriteAnimal"])
    for rec in records:
        writer.writerow(record)
    return [output.getvalue()]

data.mapPartitions(writeRecords).saveAsTextFile("file:///home/csvfile_path")
