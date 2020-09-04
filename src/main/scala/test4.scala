object test4 {
  def main(args: Array[String]): Unit = {
    val list: List[(Int, Int)] = List((1, 1), (2, 1), (3, 1), (1, 1))

    val intToTuples: Map[Int, List[(Int, Int)]] = list.groupBy(v => v._1)

    val intToInt: Map[Int, Int] = intToTuples.map((tuple: (Int, List[(Int, Int)])) => {
      val values: List[(Int, Int)] = tuple._2
      val finalList: List[Int] = values.map(v => v._2)
      (tuple._1, finalList.reduce((a, b) => a + b))
    })

    println(intToInt)



    val newlist = List("1", "2", "3", "4")
    val str: String = newlist.reduce((a, b) => a.concat(b))
    print(str)
  }
}
