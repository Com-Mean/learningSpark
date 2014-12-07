// Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext("local", "My App")
val input = sc.parallelize(List(1,2,3,4))

// map, single output of each element, return iterator of RDD
val result = input.map(x => x*x)
println(result.collect())

// flatMap, multiple output of each element, return RDD, not iterators
val lines = sc.parallelize(List("hello world", "hi"))
val words = lines.flatMap(line=>line.split(" "))
words.first() # return hello

// Basic RDD transformations
val rdd = sc.parallelize(List(1, 2, 3, 3))
rdd.map(x=>x+1).collect() #[2, 3, 4, 4]
rdd.flatMap(x=>x.to(3)) #[1,2,3, 2,3, 3, 3]
rdd.filter(x=>x!=1)       # [2, 3, 3]
rdd.distinct() # [1, 2, 3]
rdd.sample(False, 0.5) # sample an Rdd

// Two-RDD transformations
val la = sc.parallelize(List(1,2,3)); val lb = sc.parallelize(List(3, 4, 5))
la.union(lb) # [1,2,3,3,4,5]
la.intersection(lb) # [3]
la.subtract(lb) # [1,2]
la.cartesian(lb) # [(1,3), (1,4), (1,5), (2,3), (2, 4), (2, 5), (3, 3), (3, 4), (3, 5)]
