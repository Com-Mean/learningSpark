/*************************************************************************
    > File Name: seqenceFile.scala
    > Author: lpqiu
    > Mail: lpqiu_1018@126.com 
    > Created Time: 2014年11月30日 星期日 19时52分07秒
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
      val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWriteable]).
          map{case(x,y)=>(x.toString,y.get())}

      val data = sc.parallelize(List(("Pandan",3), ("Kay",6), ("Snail",2)))
      data.saveAsSequenceFile(outputFile)

      // load keyValueTextInputFormat
      val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile).
        map{case(x,y)=>(x.toString, y.toString)}

      // Twitter’s Elephant Bird package supports a large number of data formats, 
      // including JSON, Lucene, Protocol Buffer related formats, and so on.
      // load LZO compressed JSON with Elephant Bird.
      // LZO support requires installing the hadoop-lzo package and point Spark to its
      // native libs.
      val input = sc.newAPIHadoopFile(inputFile, classOf[LzoJsonInputFormat],
        classOf[LongWriteable],classOf[MapWriteable],conf)
      // Each MapWriteable in "input" represents a JSON object
    }
}

// Elephant Bird Protocol buffer write out example
// protocol buffer library version 2.5, the same ver as Spark
object PBWriteOuter{
  def main(args: Array[String]){
      val conf = new SparkConf().setMaster("local").setAppName("My App")
      val sc = new SparkContext("local", "My App")
      val job = new Job()
      val conf = job.getConfiguration
      LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);
      val dnaLouge = Places.Venue.newBuilder()
      dnaLouge.SetId(1)
      dnaLouge.setName("DNA Lounge")
      dnaLouge.setType(Places.Venue.VenueType.CLUB)
      val data = sc.parallelize(List(dnaLouge.build()))
      val outputData = data.map{pb=>
        val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
        protoWritable.set(pb)
        (null, protoWritable)
      }
      outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text],
        classOf[ProtobufWritable[Places.Venue]],
        classOf[LzoProtobufBlockOutputFormat[protoWritable[Places.Venue]]],conf)
  }
}
