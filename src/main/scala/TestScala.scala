import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TestScala {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("testMapPartition").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val rdd: RDD[Int] = ssc.sparkContext.makeRDD(mockData())

    val inputStream: InputDStream[Int] = ssc.queueStream(mutable.Queue(rdd))

    val stream: DStream[Int] = inputStream.mapPartitions(iter => {
      println(iter.toList)
      iter
    })
    stream.print()



    ssc.start()
    ssc.awaitTermination()
  }

  def mockData(): List[Int] ={
    val buffer: ListBuffer[Int] = ListBuffer[Int]()
    for (i <- 1 to 100){
      buffer += i
    }
    buffer.toList
  }
}
