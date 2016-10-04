package language.similarity

import options.{Parameters, ParamsWord2Vec}
import options.Resources.{DocumentResources, Word2VecResources}
import org.apache.spark.SparkContext
import org.apache.spark.ml.MainExecutor
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.DenseVector

import scala.io.Source
import scala.util.control.Breaks

/**
  * Created by wolf on 02.10.2016.
  */

class VectorSimilarity(word2VecModel: Word2VecModel) extends Serializable {
  val wordVecMap = word2VecModel.getVectors

  def dot(w1: Array[Float], w2: Array[Float]): Double = {
    require(w1.length == w2.length, "Word vector sizes must be equal...")
    w1.zip(w2).foldLeft[Double](0.0) { case (sum, (d1, d2)) => {
      sum + d1 * d2
    }
    }
  }

  def cosine(w1: Array[Float], w2: Array[Float]): Double = {
    require(w1.length == w2.length, "Word vector sizes must be equal...")
    val tuple = w1.zip(w2).foldLeft[(Double, Double, Double)]((0d, 0d, 0d)) { case (sum, (d1, d2)) => {
      val dotSum = sum._1 + d1 * d2
      val norm1 = sum._2 + d1 * d1
      val norm2 = sum._3 + d2 * d2
      (dotSum, norm1, norm2)
    }
    }

    tuple._1 / (math.sqrt(tuple._2) * math.sqrt(tuple._3))
  }

  def vectorize(sentence: Seq[String]): Array[(Array[Float], Int)] = {
    var array = Array[(Array[Float], Int)]()

    sentence.zipWithIndex.foreach { case (word, index) => {
      if (wordVecMap.contains(word)) {
        array = array :+ (wordVecMap.get(word).get, index)
      }
    }
    }

    array
  }

}

abstract class SentenceSimilarity(word2vecModel: Word2VecModel) extends VectorSimilarity(word2vecModel) {

  val similarity = new VectorSimilarity(word2vecModel)

  def prepare(sentence:Seq[String]):Seq[String]={
    sentence.filter(word=>{!word.matches("\\p{Punct}")}).map(word=>{word.toLowerCase(Parameters.crrLocale)})
  }

  def longestShortest(w1: Seq[String], w2: Seq[String]): (Seq[String], Seq[String]) = {
    if (w1.length > w2.length) (w1, w2)
    else (w2, w1)
  }

  def similarityVector(s1: Seq[String], s2: Seq[String]): Array[Double]

  def similarity(s1: Seq[String], s2: Seq[String]): Double

}

class KenterSimilarity(word2VecModel: Word2VecModel, avgSenLen: Int) extends SentenceSimilarity(word2VecModel) {

  var b = 1
  var k1 = 3

  override def similarityVector(s1: Seq[String], s2: Seq[String]): Array[Double] = {
    val longShort = longestShortest(s1, s2)
    val sLong = longShort._1
    val sShort = longShort._2
    val sShortLength = sShort.length.toDouble

    val s1Vector = vectorize(sLong)
    val s2Vector = vectorize(sShort)
    var simVector = Array[Double]()

    if(s2Vector.length == 0) {
      simVector = simVector :+ 0.0
    }
    else {
      s1Vector.foreach { case (v1, ind) => {
        //TODO IDF of ind from v1

        val combinations = for (v2 <- s2Vector) yield (v1, v2._1)
        val max = combinations.map { case (x, y) => cosine(x, y) }.max[Double]
        val sim = (max * (k1 + 1) / (max + k1 * (1 - b + b * (sShortLength / avgSenLen))))
        simVector = simVector :+ sim
      }
      }
    }

    simVector
  }

  override def similarity(s1:Seq[String], s2:Seq[String]):Double={
    val vector = similarityVector(s1,s2)
    vector.sum
  }
}

object SentenceSimilarity{

  val sc = new MainExecutor().sc
  val word2VecModel = Word2VecLoader.loadFromTxt(Word2VecResources.word2VecTxt)
  val similarityMethod = new KenterSimilarity(word2VecModel,5)

  def main(args: Array[String]): Unit = {

    Breaks.breakable{
      while(true){
        println("Exit? (Y/N)")
        val line = Console.readLine().trim
        if(line.equals("Y")) Breaks.break()
        else{
          println("Enter source sentence...")
          val s1 = Console.readLine().split("\\s+")
          println("Enter destination sentence...")
          val s2 = Console.readLine().split("\\s+")

          println("Similarity: "+similarityMethod.similarity(s1,s2))
        }
      }
    }

  }
}

object Word2VecLoader {

  def loadFromDF(sc:SparkContext, filename:String):Word2VecModel={
    Word2VecModel.load(sc, filename)
  }

  def loadFromTxt(filename:String):Word2VecModel={
    val map = Source.fromFile(filename).getLines()
    .map(line=>line.split("\\s")).map(line=>{
      val word = line(0)
      val array = line.slice(1, line.length)
        .map(weight=>weight.toFloat)

      (word, array)
    }).toMap

    new Word2VecModel(map)
  }

  def main(args: Array[String]): Unit = {

  }
}


