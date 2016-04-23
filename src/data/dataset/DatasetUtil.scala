package data.dataset

import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 05.04.2016.
  */
object DatasetUtil {
  def computePercentile(rdd: RDD[Int], tile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val r = rdd.sortBy(x => x)
    val c = r.count()
    if (c == 1) r.first()
    else {
      val n = (tile / 100d) * (c + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
  }


}
