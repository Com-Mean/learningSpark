/*************************************************************************
    > File Name: initSpark.java
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: Fri Nov 14 02:24:15 2014
 ************************************************************************/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
JavaSparkContext sc = new JavaSparkContext(conf);
