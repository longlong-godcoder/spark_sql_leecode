package sparksql.hard

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**

  求高峰期记录！

X 市建了一个新的体育馆，每日人流量信息被记录在这三列信息中：**序号** (id)、**日期** (visit_date)、 **人流量** (people)。
请编写一个查询语句，找出人流量的高峰期。高峰期时，至少连续三行记录中的人流量不少于100。
例如，表 `stadium`：
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 1    | 2017-01-01 | 10        |
| 2    | 2017-01-02 | 109       |
| 3    | 2017-01-03 | 150       |
| 4    | 2017-01-04 | 99        |
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+

对于上面的示例数据，输出为：
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
  **提示：**
每天只有一行记录，日期随着 id 的增加而增加。

  */

object test_601_human_traffic_of_stadium {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestApp")
      .master("local[4]")
      .getOrCreate()

    test(spark)

    spark.stop()
  }

  def test(spark: SparkSession): Unit ={
    import spark.implicits._
    val list = List(
      Record(1, "2017-01-01", 10),
      Record(2, "2017-01-02", 109),
      Record(3, "2017-01-03", 150),
      Record(4, "2017-01-04", 99),
      Record(5, "2017-01-05", 145),
      Record(6, "2017-01-06", 1455),
      Record(7, "2017-01-07", 199),
      Record(8, "2017-01-08", 188)
    )
    val rdd: RDD[Record] = spark.sparkContext.makeRDD(list)

    val df: DataFrame = rdd.toDF()
    df.createOrReplaceTempView("stadium")
    spark.sql(
      """
        |select id, visit_date, people from
        |(
        |   select
        |   id,
        |   lead(people, 1) over(order by id) ld,
        |   lead(people, 2) over(order by id) ld2,
        |   visit_date,
        |   lag(people,1) over(order by id) lg,
        |   lag(people,2) over(order by id) lg2,
        |   people
        |   from stadium
        |
        |   ) a
        |where (a.ld >= 100 and a.lg >= 100 and a.people >= 100)
        |or (a.ld >= 100 and a.ld2 >= 100 and a.people >= 100)
        |or ((a.lg >= 100 and a.lg2 >= 100 and a.people >= 100))
      """.stripMargin).show()//最后的where条件两个是边界数据

 }

  case class Record(
                     id: Int = 0,
                     visit_date: String = "",
                     people: Int = 0
                   )
}
