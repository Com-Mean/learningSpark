// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Test {
  def main(args: Array[String])
  {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext("local", "My App")
    println(InchesToCentimeters(2))
    println(MilesToKilometers(2))
    println(GallonsToLiters(2))
  }
}
