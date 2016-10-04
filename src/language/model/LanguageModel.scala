package language.model

import java.util

import edu.berkeley.nlp.lm.NgramLanguageModel
import edu.berkeley.nlp.lm.io.LmReaders
import language.util.TextSlicer
import options.Resources
import options.Resources.LMResource
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.JavaConversions

/**
  * Created by wolf on 31.03.2016.
  */
class LanguageModel(filename: String) {


  val lm = LmReaders.readLmBinary[String](filename)


  def score(sentence: String): Double = {
    val ngrams = toList(sentence)
    lm.scoreSentence(ngrams)
  }

  def scoreTokens(sentence: Seq[String]): Double = {
    val ngrams = JavaConversions.seqAsJavaList(sentence)
    lm.scoreSentence(ngrams)
  }

  def scoreMax(sentences: Seq[String]): String = {
    val max = sentences.zipWithIndex.map { case (sentence: String, index: Int) => {
      (sentence, score(sentence))
    }
    }.maxBy(pair => pair._2)
    max._1
  }

  def scoreTokensMax(sentences: Seq[Seq[String]]): String = {
    val max = sentences.zipWithIndex.map { case (sentence, index) => {
      (sentence, scoreTokens(sentence))
    }
    }.maxBy(pair => pair._2)
    max._1.mkString(" ")
  }

  protected def toList(sentence: String): java.util.List[String] = {
    val tokens = sentence.split("\\s")
    JavaConversions.seqAsJavaList(tokens)
  }
}

class PositionalModel(val chunkNum: Int) {

  val genres = Array[String]("art","cinema","technology","music","politics")
  val modelFilenames = genres.map(name=>LMResource.modelsDir+name+".bin")

  val models = modelFilenames.map(filename => {
    new LanguageModel(filename)
  })



  /**
    *
    * @param sentences sentences
    * @return a vector of scores for several sentences
    */
  def scoreRegular(sentences: Seq[String]): Array[Vector[Double]] = {
    val chunks = TextSlicer.chunkRegular(sentences, chunkNum)

    models.map(model => {
      chunks.map(group => {
        val avg = group.foldLeft[Double](0.0){case (sum,sentence) => {
           sum + model.score(sentence)/group.length
        }}
        math.abs(avg)
      }).toVector
    })
  }



}

object LanguageModel {

  val languageModel = new LanguageModel(Resources.LMResource.modelLM5TRFilename)

  def main(args: Array[String]) {

    val sentences = Seq[String]("kayıptan haber alındı.", "kayıp çocuk", "ben bilmem.", "bilen bilir.", "nedendir bilinmez.",
      "ondandır.")

    new PositionalModel(1).scoreRegular(sentences).foreach(arr => println(arr.mkString(" ")))


    /*val sc1 = languageModel.score("lardan")
    val sc2 = languageModel.score("değerli basın mensupları")
    println(sc1)
    println(sc2)*/
  }
}
