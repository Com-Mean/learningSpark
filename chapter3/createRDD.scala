// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext("local", "My App")
//Create RDD by loading external dataset
val lines = sc.textFile("README.md")
//Create RDD by parallelize the collection in a driver program
val lines = sc.parallelize(list("pandas", "i like pandas"))
