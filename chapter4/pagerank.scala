/*************************************************************************
    > File Name: pagerank.scala
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: 2014年11月23日 星期日 18时00分09秒
 ************************************************************************/

import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._


object Test {
    def main(args: Array[String]) {
      val sc = new SparkContext()

      // Assume that our neighbor list was saved as a Spark objectFile
      val links = sc.objectFile[(String,Seq[String])]("links")
                       .partitionBy(new HashPartitioner(100))
                       .persist()

      // Initialize each page's rank to 1.0; since we use mapValues, 
      // the result RDD will have the same partitioner.
      var ranks = links.mapValues(_=>1.0)

      // Run 10 iterations of PageRank
      for (i<- 0 until 10){
        val contributions = links.join(ranks).flatMap{
          case (pageId,(links,rank))=>links.map(dest=>(dest,rank/links.size))
        }
      }
      ranks = contributions.reduceByKey(_+_).mapValues(0.15 + 0.85*_)

      // Write out the final ranks
      ranks.saveAsTextFile("ranks")
    }
}
