// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._

object Test {
  def main(args: Array[String])
  {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext("local", "My App")
    val nums = sc.parallelize(List(1,2,3,4))
    nums.reduce((x,y)=>x+y)
    nums.fold(0)((x,y)=>x+y)
    nums.fold(1)((x,y)=>x*y)
    val sumCount = nums.aggregate(0)((x,y)=>(x._1+y, x._2+1),(x,y)=>(x._1+y._1, x._2+y._2))
    val avg = sumCount._1 / sumCount._2.toDouble

    // basic actions


    // key-value pairs actions
    val sumCount = nums.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))

    val input = sc.textFile("s3://...")
    val words = input.flatMap(x=>x.split(" "))
    val result = words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)

    val input = sc.parallelize(List(("coffee",1), ("coffee", 2), ("pandan", 4)))
    val result = input.combineByKey(
      (v)=>(v,1),
      (acc:(Int, Int),v)=>(acc._1+v, acc._2+1),
      (acc1:(Int, Int),acc2:(Int,Int))=>(acc1._1+acc2._1, acc1._2+acc2._2)).map{case(key,value)=>(key, value._1/value._2.toFloat)}
    result.collectAsMap().map(println(_))

    // ReduceByKey with custom level of parallelism 
    val data = Seq(("a",3), ("b",4), ("a",1))
    sc.parallelize(data).reduceByKey(_+_)      // default parallelism
    sc.parallelize(data).reduceByKey(_+_, 10)  // custom parallelism

    // sort rdd by key
    val input:RDD[(int,Venue)]=...
    implicit val sortIntegersByString = new Ordering[Int]{
      override def compare(a:Int,b:Int)= a.toString.compare(b.toString)
    }
    rdd.sortByKey()
  }
}
