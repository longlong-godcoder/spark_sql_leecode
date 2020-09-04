object Test5 {

  def main(args: Array[String]): Unit = {
    val iter: Iterable[Int] = List[Int](1, 2, 3, 4, 5, 6)



    val tuples: Iterable[(Int, Int)] = List[(Int, Int)]((1, 1), (1, 2), (2, 3), (2, 3))
    val map: Map[Int, Int] = tuples.toMap


    val score: Long = 3543
    val hour: Double = 23434
    val d: Double = hour - score
    println(d)
    val result: Double = score.toDouble / hour



    val str: String = result.formatted("%.2f")


    val r: Double = score / hour
    println(r)
//    val newList: List[Int] = iterator.toList


  }
  //1为向下取整数，0为保留两位小数
  def func(value: Long, isRoundDown: Int): Unit ={
    if (isRoundDown == 1){

    }
  }
}
