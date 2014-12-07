/*************************************************************************
    > File Name: loadFiles.scala
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: 2014年11月23日 星期日 20时12分30秒
 ************************************************************************/

// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._

object Test {
    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("My App")
      val sc = new SparkContext("local", "My App")
      val input = sc.wholeTextFiles("file://home/spark/README.md")
      val result = input.mapValues{y=>
          val nums = y.split(" ").map(x=>x.toDouble)
          nums.sum/nums.size.toDouble
      }
      result.saveAsTextFile("/home/spark/work_dir")
    }
}
