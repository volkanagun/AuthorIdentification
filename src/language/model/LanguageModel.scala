package language.model

import java.util

import edu.berkeley.nlp.lm.io.LmReaders
import options.Resources

import scala.collection.JavaConversions

/**
  * Created by wolf on 31.03.2016.
  */
class LanguageModel(ngram: Int) {

  val binaryFilename = Resources.LanguageModel.modelLM5TRFilename
  val lm = LmReaders.readLmBinary[String](binaryFilename)

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

object LanguageModel {

  val languageModel = new LanguageModel(5)

  def main(args: Array[String]) {
    val sc1 = languageModel.score("evli adam")
    val sc2 = languageModel.score("değerli basın mensupları")
    println(sc1)
    println(sc2)
  }
}
