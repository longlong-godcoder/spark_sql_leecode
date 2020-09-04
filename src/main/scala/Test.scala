import jodd.util.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Test {

  val is_completed = "is_completed"
  val completed_date = "completed_date"
  val expiration_date = "expiration_date"

  val func  = (is_completed: String, completed_date: String, expiration_date: String) => {
    var num = 2
    if ("1".equals(is_completed) && notBlank(completed_date) && notBlank(expiration_date) && completed_date > expiration_date){
      num = 1
    }else if("1".equals(is_completed) && notBlank(completed_date) && notBlank(expiration_date)){
      num = 0
    }
    num
  }
  def notBlank(str: String): Boolean ={
    StringUtil.isNotBlank(str) && !str.toLowerCase().equals("null")
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestApp")
      .master("local[4]")
      .getOrCreate()



    import spark.implicits._

    val function: UserDefinedFunction = udf(func)

    val studentRDD: RDD[Student] = spark.sparkContext.makeRDD(make())

    val studentDF: DataFrame = studentRDD.toDF()

//    val resultF: DataFrame = df.
//      withColumn("is_overdue",
//        when(col(s"$is_completed").equalTo(1).and(col(s"$expiration_date").isNotNull).and(col(s"$expiration_date").notEqual("null")).and(col(s"$completed_date").isNotNull).and(col(s"$completed_date").notEqual("null")).and(col(s"$completed_date").>(col(s"$expiration_date"))), 1)
//          .when(col(s"$is_completed").equalTo(1).and(col(s"$expiration_date").isNotNull).and(col(s"$expiration_date").notEqual("null")).and(col(s"$completed_date").isNotNull).and(col(s"$completed_date").notEqual("null")), 0)
//          .otherwise(2)
//      )
    val resultDF: DataFrame = studentDF.withColumn("is_overdue", function($"is_completed", $"completed_date", $"expiration_date"))


    resultDF.show()



    spark.close()
  }

  def make(): List[Student] ={
    List(
      Student("1", "longlong", "2020-07-04 13:30:00", "2020-07-04 13:30:00", "1"),
      Student("3", "lisi", "2020-07-04 13:30:00", "2020-07-04 13:20:00", "1"),
      Student("4", "wangwu", "2020-07-04 13:30:00", "2020-07-04 13:40:00", "1"),
      Student("5", "zehua", null , "2020-07-04 13:30:00", "1"),
      Student("6", "guofu", "2020-07-04 13:30:00", null, "1"),
      Student("7", "faxing", "2020-07-04 13:30:00", "2020-07-04 13:20:00","0"),
      Student("8", "hehe", "2020-07-04 13:30:00", null, "0"),
      Student("9", "dingbin", null, "2020-07-04 13:20:00", "0"),
      Student("10", "zhangwei", "2020-07-04 13:30:00", "2020-07-04 13:20:00", null),
      Student("11", "xiaodong", null, null, "0"),
      Student("12", "pizza", "null", null, "0"),
      Student("13", "hot", null, "null", "0"),
      Student("14", "dzyye", "", null, "0"),
      Student("15", "hello", null, "", "0"),
      Student("16", "target", null, null, "1"),
      Student("17", "pom", "null", "null", "0"),
      Student("18", "zealer", "null", "null", "1")

    )
  }

  def getSchema(): StructType ={
    StructType(
      Array(
        StructField("id", StringType),
        StructField("name", StringType)
      )
    )
  }

  case class Student(
                    var id: String = "",
                    var name: String = "",
                    var expiration_date: String = null,
                    var completed_date: String = null,
                    var is_completed: String = null
                    )

}
