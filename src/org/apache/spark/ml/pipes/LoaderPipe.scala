package org.apache.spark.ml.pipes

import java.io.File
import java.util.logging.{Level, Logger}

import data.dataset.{DatasetLoader, DatasetPrepare, DatasetStats}
import data.document.{Document, Paragraph, Sentence}
import language.boundary.SentenceML
import language.morphology._
import language.postagger.PosTaggerCRF
import language.tokenization.MyTokenizer
import options.Resources._
import options._
import org.apache.hadoop.yarn.webapp.Params
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wolf on 23.04.2016.
  */
class LoaderPipe {

  val paritionSize = 12
  //Encapsulation of a pipeline
  //Paragraph segmenting, sentence and token segmenting

  //Consider each instance by segmented item and author
  //So a document pipe is text and author
  //So a document pipe is paragraph and author
  //So a document pipe is sentence,tokens or morphs and author

  def loadSentences(sql: SQLContext, dataset: RDD[Document]): DataFrame = {
    import sql.implicits._

    dataset.flatMap(document => {
      new Segmentor().sentences(document)
    }).toDF
  }

  def loadParagraphs(sql: SQLContext, dataset: RDD[Document]): DataFrame = {
    import sql.implicits._

    dataset.flatMap(document => {
      new Segmentor().paragraphs(document)
    }).toDF
  }


  def loadTokens(sql: SQLContext, dataset: RDD[Document]): DataFrame = {
    import sql.implicits._

    dataset.map(document => {
      val segmentor = new Segmentor()
      val tokens = segmentor.tokens(document)
      val morphs = segmentor.morphs(document)
      (document.docid, document.author, tokens.toVector, morphs.toVector)
    }).toDF(LoaderPipe.docidCol, LoaderPipe.authorCol, LoaderPipe.tokensCol, LoaderPipe.morphsCol)
  }

  def loadFlat(sql: SQLContext, dataset: RDD[Document]): DataFrame = {
    import sql.implicits._
    //Group use mapPartitions
    val segmentorBrcst = sql.sparkContext.broadcast(new Segmentor())
    dataset.mapPartitions(documentIter => {
      val documentVec = documentIter.toVector
      val segmentor = segmentorBrcst.value
      documentVec.map(document => {
        println("Starting doc " + document.doctype)
        val title = segmentor.title(document)
        val sentences = segmentor.sentences(document).map(sentence => sentence.text)
        val sentenceTokens = sentences.map(sentence => segmentor.tokens(sentence))

        val paragraphs = segmentor
          .paragraphs(document)
          .map(paragraph => paragraph.text)

        val tokens = sentenceTokens.flatMap(seq => seq)
        val posses = segmentor.possesTokenized(sentenceTokens)
        val morphSequence = segmentor.morphsSequence(sentenceTokens)
        val morphs = morphSequence.flatten


        (document.docid, document.doctype, document.genre, document.author, document.title, document.text,
          paragraphs.toVector, sentences, sentenceTokens.toVector, tokens.toVector, posses.toVector, morphs.toVector/*, morphSequence*/)
      }).toIterator
    }).toDF(LoaderPipe.docidCol, LoaderPipe.doctypeCol, LoaderPipe.genreCol, LoaderPipe.authorCol, LoaderPipe.titleCol, LoaderPipe.textCol,
      LoaderPipe.paragraphsCol, LoaderPipe.sentencesCol, LoaderPipe.sentenceTokensCol, LoaderPipe.tokensCol, LoaderPipe.possesCol, LoaderPipe.morphsCol/*, LoaderPipe.morphSeqCol*/)
  }


  def loadFlat(sql: SQLContext, dataset: RDD[Document], filename: String): DataFrame = {
    if (new File(filename).exists()) {
      sql.read.load(filename)
    }
    else {
      val df = loadFlat(sql, dataset)
      df.write.save(filename)
      df
    }
  }

  //Do all the job here
  //1. Create an Sampled RDD if necessary (rdd folder)
  //2. Create an DF if necessary (sampled DF folder)
  //3. Create a processed DF if necessary (processed folder) - No pipeline execution
  /*def loadFlat(sqlContext: SQLContext): DataFrame = {

    println("Loading dataset...")

    val processedTrainFilename = ParamsUnique.processedTrainFilename()
    val processedTestFilename = ParamsUnique.processedTestFilename()

    val sampledDFFilename = ParamsUnique.sampledDFFilename()
    val sampledRDDFilename = ParamsUnique.sampledRDDFilename()

    val processedTrainExists = DatasetLoader.exists(processedTrainFilename)
    val processedTestExists = DatasetLoader.exists(processedTestFilename)
    val sampledDFExists = DatasetLoader.exists(sampledDFFilename)
    val sampledRDDExists = DatasetLoader.exists(sampledRDDFilename)



    if (sampledDFExists) {
      sqlContext.read.load(sampledDFFilename)
    }
    else if (sampledRDDExists) {
      val rdd = DatasetLoader.loadObjectDocs(sqlContext.sparkContext, sampledRDDFilename)
      val df = loadFlat(sqlContext, rdd)
      df.write.save(sampledDFFilename)
      df
    }
    else {
      val rdd = DatasetPrepare.sampledNonRate(sqlContext.sparkContext, sampledRDDFilename)
      val df = loadFlat(sqlContext, rdd)
      df.write.save(sampledDFFilename)
      df
    }
  }*/

  def loadFlatSentencesDF(sql: SQLContext, isBig:Boolean): DataFrame = {
    import sql.implicits._
    println("Loading sentences dataset...")
    val sc = sql.sparkContext
    val filename = if(isBig)  DatasetResources.sentenceLargeDFDir else DatasetResources.sentenceDFDir

    if (DatasetLoader.exists(filename)) {
      sql.read.load(filename)
    }
    else {
      val sentenceFilename = if(isBig) DocumentResources.largeSentences else DocumentResources.smallSentences
      val rdd = sc.textFile(sentenceFilename, 64)
      val segmentorBrcst = sql.sparkContext.broadcast(new Segmentor())

      val mrdd = rdd.mapPartitions(sentenceIter => {
        val sentenceVec = sentenceIter.toVector
        val segmentor = segmentorBrcst.value
        sentenceVec.map(sentence => {
          val tokens = segmentor.tokens(sentence)

          (sentence.hashCode, tokens)
        }).toIterator
      })

      val df = mrdd.toDF(LoaderPipe.docidCol, LoaderPipe.tokensCol)
      df.write.save(filename)
      df
    }
  }


  def loadSentencesDF(sql: SQLContext, isBig:Boolean): DataFrame = {
    import sql.implicits._
    println("Loading sentences dataset...")
    val sc = sql.sparkContext
    val filename = if(isBig)  DatasetResources.sentenceLargeDFDir else DatasetResources.sentenceDFDir

    if (DatasetLoader.exists(filename)) {
      sql.read.load(filename)
    }
    else {
      val sentenceFilename = if(isBig) DocumentResources.largeSentences else DocumentResources.smallSentences
      val rdd = sc.textFile(sentenceFilename, 64)
      val segmentorBrcst = sql.sparkContext.broadcast(new Segmentor())

      val mrdd = rdd.mapPartitions(sentenceIter => {
        val sentenceVec = sentenceIter.toVector
        val segmentor = segmentorBrcst.value
        sentenceVec.map(sentence => {
          val tokens = segmentor.tokens(sentence)
          val posses = segmentor.posses(tokens)
          /*val lwtokens = tokens.map(token=>token.toUpperCase(Parameters.crrLocale))*/
          (tokens, posses)
        }).toIterator
      })

      val df = mrdd.toDF(LoaderPipe.tokensCol, LoaderPipe.possesCol)
      df.write.save(filename)
      df
    }
  }

  def loadFlatDF(sqlContext: SQLContext): DataFrame = {

    println("Loading dataset...")


    val sampledDFFilename = ParamsUnique.sampledDFFilename()
    val sampledStatsFilename = ParamsUnique.statisticsDFFilename()
    val sampledRDDFilename = ParamsUnique.sampledRDDFilename()

    val sampledDFExists = DatasetLoader.exists(sampledDFFilename)


    if (sampledDFExists) {
      sqlContext.read.load(sampledDFFilename)
    }
    else {
      val df = DatasetPrepare.sampledFlatDF(sqlContext.sparkContext, sampledRDDFilename)
      df.write.save(sampledDFFilename)
      DatasetStats.writeAsXML(df, sampledStatsFilename)
      sqlContext.read.load(sampledDFFilename)
    }
  }


  def loadAggregatedFlat(sql: SQLContext, dataset: RDD[Document]): DataFrame = {
    import sql.implicits._
    dataset.groupBy(document => document.author)
      .map { case (author, docs) => {

        val segmentor = new Segmentor
        var text = ""
        var paragraphs = Seq[String]()
        var sentences = Seq[String]()
        var sentenceTokens = Seq[Seq[String]]()
        var tokens = Seq[String]()
        var posses = Seq[String]()
        var morphs = Seq[MorphLight]()

        for (document <- docs) {

          text += "\n" + document.text
          paragraphs = paragraphs ++ segmentor.paragraphs(document).map(paragraph => paragraph.text)
          sentences = sentences ++ segmentor.sentences(document).map(sentence => sentence.text)
          sentenceTokens = sentenceTokens ++ sentences.map(sentence => segmentor.tokens(sentence))
          tokens = tokens ++ segmentor.tokens(document)
          posses = posses ++ segmentor.posses(document)
          morphs = morphs ++ Seq[MorphLight]()

        }

        (author, text, paragraphs, sentences, sentenceTokens, tokens, posses, morphs)

      }
      }.toDF(LoaderPipe.authorCol, LoaderPipe.textCol, LoaderPipe.paragraphsCol, LoaderPipe.sentencesCol,
      LoaderPipe.sentenceTokensCol, LoaderPipe.tokensCol, LoaderPipe.possesCol, LoaderPipe.morphsCol)

  }

}


object LoaderPipe {
  val sc = null
  //initLocal
  val pipe = new LoaderPipe()

  var docidCol = "docid"
  var doctypeCol = "doctype"
  var genreCol = "genre"
  var authorCol = "author"
  var titleCol = "title"
  var textCol = "text"

  var paragraphsCol = "paragraphs"
  var sentencesCol = "sentences"
  var sentenceTokensCol = "sentence-tokens"
  var tokensCol = "tokens"
  var possesCol = "posses"
  var morphsCol = "morphs"
  var morphSeqCol = "morphsequence"

  def initLocal: SparkContext = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("log4j.rootCategory").setLevel(Level.OFF)
    val conf: SparkConf = new SparkConf()
      .setAppName("Stemming Parsing")
      .setMaster("local[24]")
      .set("spark.default.parallelism", "24")
      .set("log4j.rootCategory", "ERROR")
      .set("spark.executor.memory", "12g")
      .set("spark.driver.maxResultSize", "24g")
      .set("spark.driver.memory", "72g")

    new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val sql = null //nullnew SQLContext(sc)
    val rdd = DatasetLoader.loadBlogDocs(sc, "n-11")
    val df = pipe.loadFlat(sql, rdd)
    df.select("sentence-tokens").show(5, false)
    val grouped = pipe.loadAggregatedFlat(sql, rdd)
    grouped.select("tokens").show(5, false)
  }

}

class Segmentor() extends Serializable {
  //Get document
  //Create segments of text
  //Paragraphs, Sentences, Tokens, Morphs, Posses
  val sentenceML = new SentenceML
  val myTokenizer = new MyTokenizer
  val analyzer = new HasimAnalyzerImp(new EmptyTagModel)
  val posTagger = new PosTaggerCRF(PennPosTagSet, MorphologyResources.crfPennPosTag)
  val morphTagger = new MorphTagger()

  def paragraphs(document: Document): Seq[Paragraph] = {
    document.toParagraphs()
  }

  def sentences(document: Document): Seq[Sentence] = {
    document.toSentences(sentenceML)
  }

  def tokens(document: Document): Seq[String] = {
    document.toTokens(sentenceML, myTokenizer)
  }

  def tokens(sentence: String): Seq[String] = {
    myTokenizer.tokenize(sentence)
  }

  def morphs(document: Document): Seq[String] = {
    document.toMorphs(sentenceML, myTokenizer, morphTagger)
  }

  def morphs(sentence: String): Seq[String] = {
    val tokens = myTokenizer.tokenize(sentence)
    morphTagger.predictTags(tokens).flatten
  }

  protected def morphsFromTokens(tokens: Seq[String]): Seq[String] = {
    morphTagger.predictTags(tokens).flatten
  }

  protected def morphsSeqFromTokens(tokens: Seq[String]): Seq[Seq[String]] = {
    morphTagger.predictTags(tokens)
  }

  def morphs(sentences: Seq[String]): Seq[String] = {
    sentences.flatMap(sentence => morphs(sentence))
  }

  def morphsTokenized(sentences: Seq[Seq[String]]): Seq[String] = {
    sentences.flatMap(tokens => morphsFromTokens(tokens))
  }

  def morphsSequence(sentences: Seq[Seq[String]]): Seq[Seq[String]] = {
    sentences.flatMap(tokens => morphsSeqFromTokens(tokens))
  }


  def posses(document: Document): Seq[String] = {
    document.toPosses(sentenceML, myTokenizer, posTagger)
  }

  def posses(sentence: String): Seq[String] = {
    val tokens = myTokenizer.tokenize(sentence)
    posTagger.test(tokens)
  }

  def possesFromToks(tokens: Seq[String]): Seq[String] = {
    posTagger.test(tokens)
  }

  def posses(sentences: Seq[String]): Seq[String] = {
    sentences.flatMap(sentence => posses(sentence))
  }

  def possesTokenized(sentences: Seq[Seq[String]]): Seq[String] = {
    sentences.flatMap(tokens => possesFromToks(tokens))
  }

  def title(document: Document): String = {
    if (document.isTweet()) document.text
    else document.title
  }
}

class Masking() extends Serializable {
  //pattern masking
  //url, namedentity type, anything that you can annotate
  //change urls->domain-url, dates to datetypes, time to timetypes

  //CCGParser can be used for advanced recognition and parsing with learning
  //However it needs complecated design


}