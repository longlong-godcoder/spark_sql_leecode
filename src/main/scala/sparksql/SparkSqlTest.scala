package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkSqlTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestApp")
      .master("local[4]")
      .getOrCreate()

    test1(spark)

    spark.stop()
  }

  def test1(spark: SparkSession): Unit ={
    import spark.implicits._
    val list1: List[Student] = List[Student](Student("1", "longlong1"), Student("2", "longlong2"))
    val rdd: RDD[Student] = spark.sparkContext.makeRDD(list1)

    rdd.toDF().show()
  }

  case class Student(
                    id :String = "",
                    name: String = ""
                    )

  def test2(): Unit ={

  }

}


