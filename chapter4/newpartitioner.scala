/*************************************************************************
    > File Name: newpartitioner.scala
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: 2014年11月23日 星期日 18时42分39秒
 ************************************************************************/
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._

class DomainNamePartitioner(numParts:Int) extends Partitioner{
    override def numPatitions:Int = numParts

    override def getPartition(key:Any):Int = {
      val domain = new java.net.URL(key.toString).getHost()
      val code = (domain.hashCode%numPatitions)
      if (code < 0){
        code + numPatitions    // make it non-negtive
      }else{
        code
      }
    }

    // Java equals method to let Spark cpmpare our Partitioner objects
    override def equals(other:Any):Boolean = other match{
      case dnp: DomainNamePartitioner=>dnp.numPatitions == numPatitions
      case _=>
        false
    }
}

object Test {
    def main(args: Array[String]) {
    }
}
