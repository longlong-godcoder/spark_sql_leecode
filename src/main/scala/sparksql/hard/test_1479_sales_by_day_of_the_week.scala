package sparksql.hard

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions

/**
  * #### [1479. 周内每天的销售情况](https://leetcode-cn.com/problems/sales-by-day-of-the-week/)
  **
难度困难
  **
SQL架构
  **
表：`Orders`
  **
 ```
*+---------------+---------+
*| Column Name   | Type    |
*+---------------+---------+
*| order_id      | int     |
*| customer_id   | int     |
*| order_date    | date    |
*| item_id       | varchar |
*| quantity      | int     |
*+---------------+---------+
*(order_id, item_id) 是该表主键
*该表包含了订单信息
*order_date 是id为 item_id 的商品被id为 customer_id 的消费者订购的日期.

 表：`Items`

*+---------------------+---------+
*| Column Name         | Type    |
*+---------------------+---------+
*| item_id             | varchar |
*| item_name           | varchar |
*| item_category       | varchar |
*+---------------------+---------+
*item_id 是该表主键
*item_name 是商品的名字
*item_category 是商品的类别
*```
 **
 *
 *
 你是企业主，想要获得分类商品和周内每天的销售报告。
 **
 写一个SQL语句，报告 **周内每天** 每个商品类别下订购了多少单位。
 **
 返回结果表单 **按商品类别排序** 。
 **
 查询结果格式如下例所示：
 **
 *
 *

*Orders 表：
*+------------+--------------+-------------+--------------+-------------+
*| order_id   | customer_id  | order_date  | item_id      | quantity    |
*+------------+--------------+-------------+--------------+-------------+
*| 1          | 1            | 2020-06-01  | 1            | 10          |
*| 2          | 1            | 2020-06-08  | 2            | 10          |
*| 3          | 2            | 2020-06-02  | 1            | 5           |
*| 4          | 3            | 2020-06-03  | 3            | 5           |
*| 5          | 4            | 2020-06-04  | 4            | 1           |
*| 6          | 4            | 2020-06-05  | 5            | 5           |
*| 7          | 5            | 2020-06-05  | 1            | 10          |
*| 8          | 5            | 2020-06-14  | 4            | 5           |
*| 9          | 5            | 2020-06-21  | 3            | 5           |
*+------------+--------------+-------------+--------------+-------------+
 **
 Items 表：
*+------------+----------------+---------------+
*| item_id    | item_name      | item_category |
*+------------+----------------+---------------+
*| 1          | LC Alg. Book   | Book          |
*| 2          | LC DB. Book    | Book          |
*| 3          | LC SmarthPhone | Phone         |
*| 4          | LC Phone 2020  | Phone         |
*| 5          | LC SmartGlass  | Glasses       |
*| 6          | LC T-Shirt XL  | T-Shirt       |
*+------------+----------------+---------------+
 **
 Result 表：
*+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
*| Category   | Monday    | Tuesday   | Wednesday | Thursday  | Friday    | Saturday  | Sunday    |
*+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
*| Book       | 20        | 5         | 0         | 0         | 10        | 0         | 0         |
*| Glasses    | 0         | 0         | 0         | 0         | 5         | 0         | 0         |
*| Phone      | 0         | 0         | 5         | 1         | 0         | 0         | 10        |
*| T-Shirt    | 0         | 0         | 0         | 0         | 0         | 0         | 0         |
*+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
*在周一(2020-06-01, 2020-06-08)，Book分类(ids: 1, 2)下，总共销售了20个单位(10 + 10)
*在周二(2020-06-02)，Book分类(ids: 1, 2)下，总共销售了5个单位
*在周三(2020-06-03)，Phone分类(ids: 3, 4)下，总共销售了5个单位
*在周四(2020-06-04)，Phone分类(ids: 3, 4)下，总共销售了1个单位
*在周五(2020-06-05)，Book分类(ids: 1, 2)下，总共销售了10个单位，Glasses分类(ids: 5)下，总共销售了5个单位
*在周六, 没有商品销售
*在周天(2020-06-14, 2020-06-21)，Phone分类(ids: 3, 4)下，总共销售了10个单位(5 + 5)
*没有销售 T-Shirt 类别的商品
  **/
object test_1479_sales_by_day_of_the_week {

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
    val orders: List[Order] = List[Order](
      Order("1", "1", "2020-06-01", "1", 10),
      Order("2", "1", "2020-06-08", "2", 10),
      Order("3", "2", "2020-06-02", "1", 5),
      Order("4", "3", "2020-06-03", "3", 5),
      Order("5", "4", "2020-06-04", "4", 1),
      Order("6", "4", "2020-06-05", "5", 5),
      Order("7", "5", "2020-06-05", "1", 10),
      Order("8", "5", "2020-06-14", "4", 5),
      Order("9", "5", "2020-06-21", "3", 5)
    )

    val items: List[Item] = List[Item](
      Item("1", "LC Alg. Book", "Book"),
      Item("2", "LC DB. Book", "Book"),
      Item("3", "LC SmarthPhone", "Phone"),
      Item("4", "LC Phone 2020", "Phone"),
      Item("5", "LC SmartGlass", "Glasses"),
      Item("6", "LC T-Shirt XL", "T-Shirt")
    )

    val ordersDF: DataFrame = spark.sparkContext.makeRDD(orders).toDF()
    val itemsDF: DataFrame = spark.sparkContext.makeRDD(items).toDF()
    ordersDF.createOrReplaceTempView("orders")
    itemsDF.createOrReplaceTempView("items")

    spark.sql(
      """
        |select item_category as category,
        |sum(case when weekday = 1 then quantity else 0 end) as Monday,
        |sum(case when weekday = 2 then quantity else 0 end) as Tuesday,
        |sum(case when weekday = 3 then quantity else 0 end) as Wednesday,
        |sum(case when weekday = 4 then quantity else 0 end) as Thursday,
        |sum(case when weekday = 5 then quantity else 0 end) as Friday,
        |sum(case when weekday = 6 then quantity else 0 end) as Saturday,
        |sum(case when weekday = 7 then quantity else 0 end) as Sunday
        |from
        |(
        |   select item_category, quantity, date_format(order_date ,'u') as weekday
        |   from items left join orders on items.item_id = orders.item_id
        |) t
        |group by item_category
        |order by item_category
      """.stripMargin).show()
  }

  case class Order(
                    order_id: String = "",
                    customer_id: String = "",
                    order_date: String = "",
                    item_id: String = "",
                    quantity: Long = 0L
                  )

  case class Item(
                   item_id: String = "",
                   item_name: String = "",
                   item_category: String = ""
                 )
}
