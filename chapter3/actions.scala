
// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._


object Test {
    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("My App")
      val sc = new SparkContext("local", "My App")
      val nums = sc.parallelize(List(1,2,3,4))
      nums.reduce((x,y)=>x+y)
      nums.fold(0)((x,y)=>x+y)
      nums.fold(1)((x,y)=>x*y)
      val sumCount = nums.aggregate(0)((x,y)=>(x._1+y, x._2+1),(x,y)=>(x._1+y._1, x._2+y._2))
      val avg = sumCount._1 / sumCount._2.toDouble
    }
}
// basic actions
