package language.util

/**
  * Created by wolf on 14.09.2016.
  */
object TextSlicer {

  def binner(size: Int, numBins: Int): Array[Int] = {
    val min = size / numBins
    var mod = size % numBins
    val bins = Array.fill(numBins)(min)

    while (mod > 0) {
      for {i <- 1 until numBins - 1; if (mod > 0)} {
        bins(i) += 1
        mod -= 1
      }
    }

    bins
  }

  def chunkRegular(sentences: Seq[String], chunkNum:Int): Array[Array[String]] = {

    /*if(sentences.length > 50){
      println("Number of tokens: "+sentences.length)
    }*/

    if (sentences.length <= chunkNum) {

      val text = sentences.mkString(" ")
      val bins = binner(text.length, chunkNum)
      val array = Array.fill[Array[String]](bins.length)(Array[String]())
      var index = 0

      for (i <- 0 until bins.length) {
        val binSize = bins(i)
        val slicing = text.substring(index, index + binSize)
        array(i) = Array(slicing)
        index += binSize
      }

      array
    }
    else {
      val bins = binner(sentences.length, chunkNum)
      val array = Array.fill[Array[String]](bins.length)(Array[String]())
      var index = 0

      for (i <- 0 until bins.length) {
        val binSize = bins(i)
        val slicing = sentences.slice(index, index + binSize)
        array(i) = slicing.toArray
        index += binSize
      }

      array

    }
  }
}
