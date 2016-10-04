package util

import scala.util.Random

/**
  * Created by wolf on 20.07.2016.
  */
object Testing {
  def main(args: Array[String]): Unit = {
    val pairs = for {
      i <- 0 until 300 - 1
      j <- i + 1 until 300
    } yield (i, j)
    val arr = pairs.zipWithIndex.par.map { index => {
      val time = Random.nextInt(10000)
      for(i<-0 until time){
        val x = i+13*i
      }

      index.toString}}.toArray

    println("Is ordered...")
    arr.foreach(index=>println(index))
  }
}
