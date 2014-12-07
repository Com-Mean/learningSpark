/*************************************************************************
    > File Name: loadHive.scala
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: 2014年11月30日 星期日 21时45分37秒
 ************************************************************************/

// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.hive.HiveContext


object Test {
    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("My App")
      val sc = new SparkContext("local", "My App")
      val hiveCtx = new HiveContext(sc)
      val rows = hiveCtx.hql("SELECT key, value FROM src")
      val keys = rows.map(row => row.getInt(0))
    }
}
