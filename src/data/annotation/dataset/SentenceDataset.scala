package data.annotation.dataset

import java.io.PrintWriter

import breeze.io.TextWriter.FileWriter
import data.dataset.{DatasetLoader, TestPrepare}
import language.boundary.SentenceML
import language.tokenization.MyTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by wolf on 12.04.2016.
  */
object SentenceDataset {

  val myTokenizer = new MyTokenizer()
  val sentenceML = new SentenceML()
  //Load RDD from blogs and news
  //Take 20 from each genre
  //Find sentences by sentence boundary detection
  //Find tokens for each sentence and filter by token lengths and create valid xml
  def sentencePairs(sc: SparkContext, sizeForEachGenre: Int, minTokenLenth: Int, maxTokenLength: Int): RDD[(String, Seq[String])] = {
    val rdd = DatasetLoader.loadDocuments(sc)
    val blogOrArticles = rdd.filter(doc => {
      doc.isArticle() || doc.isBlog()
    })
    val genres = blogOrArticles.filter(doc => doc.getGenre() != null)
      .groupBy(doc => doc.getGenre())

    val genreSentences = genres.mapValues(seq => {
      var sentences = Seq[String]()
      for (doc <- seq) {
        val docText = doc.getText()
        if(docText!=null) {

          val docSentences = sentenceML.fit(docText)
          for (docSentence <- docSentences) {
            val tokens = myTokenizer.tokenize(docSentence)
            if (tokens.length >= minTokenLenth && tokens.length <= maxTokenLength) {
              sentences = sentences :+ docSentence
            }
          }
        }

      }
      sentences
    })

    val genreSizes = genreSentences.filter(pair => {
      pair._2.length >= sizeForEachGenre
    })
      .flatMap(pair => {
        Random.shuffle(pair._2).take(sizeForEachGenre)
      })

    genreSizes.map(sentence => (sentence, myTokenizer.tokenize(sentence)))

  }

  def prepareByGenre(sc:SparkContext,sizeForEachGenre:Int, minTokenLength:Int, maxTokenLength:Int, filename:String): Unit = {
    val collections = sentencePairs(sc,sizeForEachGenre, minTokenLength, maxTokenLength).collect()
    new PrintWriter(filename){
      write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
      write("<ROOT>")
      for(sindex<-0 until collections.length){
        val pair = collections(sindex)
        val sentence = pair._1
        val tokens = pair._2
        write("<SENTENCE INDEX=\""+sindex+"\">\n")
        write("<TEXT>\n"+sentence+"\n</TEXT>\n")
        for(i<-0 until tokens.length){
          val index = "INDEX=\""+i+"\""
          val tokenStr = s"<TOKEN $index>${tokens(i)}</TOKEN>\n"
          val annStr = "<ANNOTATION> </ANNOTATION>\n"
          write(tokenStr)
          write(annStr)
        }
        write("</SENTENCE>\n")
      }
      write("</ROOT>")
    }.close()
  }

  def main(args: Array[String]) {
    val sc = TestPrepare.initLocal()
    val maxTokenSize = 18
    val minTokenSize = 7
    val genreSize = 50
    prepareByGenre(sc,genreSize,minTokenSize,maxTokenSize,"byGenre50.xml")

  }
}
