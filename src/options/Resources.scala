package options

import java.io.PrintWriter
import java.util.Locale
import java.util.logging.{Level, Logger}

import language.morphology.MorphLight
import options.Resources.{DocumentResources, Global}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.reflect.io.Path
import scala.util.Try

/**
  * Created by wolf on 31.03.2016.
  */
object Resources {

  object Global {
    val osname = System.getProperty("os.name").toLowerCase
    var windows = if(osname.contains("linux")) false else true
    val resources = if(!windows) "/home/wolf/Documents/java-projects/AuthorIdentification/resources/"
    else "C:/Users/Administrator/Documents/java-projects/AuthorIdentification/resources/"

    val training = resources + "training/"
    val binary = resources + "binary/"
    val datasets = binary + "datasets/"
    val dataframes = binary + "dataframes/"
    val evaluations = resources + "evaluations/"
    val reports = resources +"reports/"
    val executables = resources +"exec/"

  }

  object LanguageDetector{
    val profiles = Global.binary + "language-detect/"
  }

  object LDAResources{
    val ldaModelDir = Global.binary + "topics/lda/"
    val ldaModelVocabularyDir = Global.binary + "topics/lda-vocab/"
  }

  object Word2VecResources{

    val word2VecTxt = Global.binary + "embeddings/word2vec.txt"
    val word2VecDir = Global.binary + "word2vec/"
  }

  object OpenSentenceBD {
    val modelFilename = Global.resources + "binary/opennlp/sentences.bin"
  }

  object OpenTokenizer {
    val modelFilename = Global.resources +"binary/opennlp/en-token.bin"
  }

  object MorphologyResources {
    val sentenceMorphTrainLarge = Global.training + "morphology/train.merge"
    val sentenceMorphTrainDev = Global.training + "morphology/data.dev.txt"
    val sentenceMorphTest = Global.training + "morphology/test.merge"
    val crfMorphoTag = Global.binary + "crf/crfMorphoTag.bin"
    val crfPosTag = Global.binary + "crf/crfPosTag.bin"
    val crfPennPosTag = Global.binary + "crf/crfPennPosTag.bin"

    val crfCaseTag = Global.binary + "crf/crfCaseTag.bin"
    val crfVerbTag = Global.binary + "crf/crfVerbTag.bin"
    val crfAggrementTag = Global.binary + "crf/crfAggrementTag.bin"
    val crfDerivationTag = Global.binary + "crf/crfDerivationTag.bin"
    val morphExecFolder = Global.executables + "morph-parser/"

    protected def owpl(line: String): MorphLight = {

      //rejiminin rejim+Noun+A3sg+P3sg+Gen rejim+Noun+A3sg+P2sg+Gen
      val sep = line.split("\\s+")
      val token = sep(0)
      val morphLight = new MorphLight(token)
      for (i <- 1 until sep.length) {
        morphLight.addAnalysis(sep(i))
      }

      morphLight.setLabel(sep(1))
      morphLight.shuffle()
      morphLight

    }

    protected def owplEnd(): MorphLight = {
      new MorphLight(".")
        .addAnalysis(".+Punc")
        .setLabel(".+Punc")
    }

    def loadMorphology(filename: String): Seq[(Int, String, Seq[MorphLight])] = {
      //load sentences
      println("Loding morphology dataset from " + filename)
      var sentenceSeq = Seq[(Int, String, Seq[MorphLight])]()
      var sentence: (Int, String, Seq[MorphLight]) = null
      var id = 0
      for (line <- Source.fromFile(filename).getLines()) {
        if (line.startsWith("<DOC>") || line.startsWith("<TITLE>") || line.startsWith("</DOC>") || line.startsWith("</TITLE>")) {
          //do nothing
        }
        else if (line.startsWith("<S>")) {
          sentence = new Tuple3[Int, String, Seq[MorphLight]](id, null, Seq[MorphLight]())
          id = id + 1
        }
        else if (line.startsWith("</S>")) {
          val seq = sentence._3 :+ owplEnd()
          sentence = sentence.copy(_3 = seq)
          sentenceSeq = sentenceSeq :+ sentence
        }
        else {

          val seq = sentence._3 :+ owpl(line)
          sentence = sentence.copy(_3 = seq)

        }
      }

      sentenceSeq
    }
  }

  object OpenNER {
    val modelPersonFilename = Global.resources + "binary/opennlp/en-ner-person.bin"
    val modelLocationFilename = Global.resources +  "binary/opennlp/en-ner-location.bin"
    val modelTimeFilename = Global.resources + "binary/opennlp/en-ner-time.bin"
    val modelPercentageFilename = Global.resources + "binary/opennlp/en-ner-percentage.bin"
    val modelMoneyFilename = Global.resources + "binary/opennlp/en-ner-money.bin"
    val modelOrganizationFilename = Global.resources + "binary/opennlp/en-ner-organization.bin"
    val modelDateFilename = Global.resources + "binary/opennlp/en-ner-date.bin"

    def saveHadoop(): Unit = {

    }

    def loadHadoop(): Unit = {

    }
  }


  object LMResource {
    val modelsDir = Global.resources + "binary/language-models/"
    val sentencesSmall = Global.resources + "sentences/sentences-small.txt"
    val sentencesLarge = Global.resources + "sentences/sentences.txt"
    val modelLMNgram = Global.resources + "binary/language-models/ngram.txt"

    val modelLM5TRFilename = Global.resources + "binary/language-models/lm-5-tr.bin"
    val modelLM5TRArpaFilename = Global.resources + "binary/language-models/arpa-tr.bin"

    def saveHadoop(): Unit = {

    }

    def loadHadoop(): Unit = {

    }
  }

  object DocumentResources {

    val ARTICLE = "ARTICLE"
    val PAN = "PAN"

    val articlePrefix = "article-text-"
    val blogPrefix = "blog-text-"
    val twitterPrefix = "twitter-"
    val panLargePrefix = "LargeTrain-"
    val panSmallPrefix = "SmallTrain-"


    val largeSentences = Global.resources + "sentences/sentences.txt"
    val smallSentences = Global.resources + "sentences/sentences-small.txt"
    val embeddingSmallSentences = Global.resources + "sentences/embedding-small.txt"
    val embeddingLargeSentences = Global.resources + "sentences/embedding-large.txt"

    val embeddingSmallTokenSentences = Global.resources + "sentences/embedding-token-small.txt"
    val embeddingSmallStemSentences = Global.resources + "sentences/embedding-stem-small.txt"
    val embeddingLargeTokenSentences = Global.resources + "sentences/embedding-token-large.txt"
    val embeddingLargeStemSentences = Global.resources + "sentences/embedding-stem-large.txt"

    val twitterDir = Global.resources + "texts/twitter/"
    val twitterXMLs = twitterDir + twitterPrefix + "*"

    val blogDir = Global.resources + "texts/blogs/"
    val blogXMLs = blogDir + blogPrefix + "*"

    val panLargeDir = Global.resources + "texts/pan/sources-large/"
    val panSmallDir = Global.resources + "texts/pan/sources-small/"
    val panLargeXMLs = panLargeDir + panLargePrefix + "*"
    val panSmallXMLs = panSmallDir + panSmallPrefix + "*"


    val articleDir = Global.resources + "texts/articles/"
    val articleXMLs = articleDir + articlePrefix + "*"
    val panXMLs = Global.resources + "texts/pan/"

    val hdfsPrefix1 = "hdfs://192.168.1.3:50010/"
    val hdfsPrefix2 = "hdfs://192.168.1.3:8020/"
    val hdfsPrefix3 = "hdfs://quickstart.cloudera:8020/user/wolf/"

    val hdfsBlogXMLs = hdfsPrefix3 + "blog-xml/"


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


    def saveToHadoop(sc: SparkContext): Unit = {
      val rdd = sc.wholeTextFiles(blogXMLs)
      rdd.saveAsTextFile(hdfsBlogXMLs)
    }

    def checkHadoop(sc: SparkContext, filename: String): Boolean = {
      true
    }


  }

  object DocumentModel {

    val TYPE = "TYPE"
    val GENRE = "GENRE"

    val PANDOC = "PAN-DOC"
    val ARTICLEDOC = "ARTICLE-DOC"
    val TWITTERDOC = "TWEET-DOC"
    val REUTERSDOC = "REUTERS-DOC"

    val TWEET = "TWEET"

    val BLOGDOC = "BLOG-DOC"

    val SPORT = "sports"
    val POLITICS = "politics"
    val TECHNOLOGY = "technology"
    val SOCIAL = "travel";
    val TWITTER = "twitter";

    def isArticle(docType: String): Boolean = {
      docType.equals(ARTICLEDOC) || docType.equals("A")
    }

    def isBlog(docType: String): Boolean = {
      docType.equals(BLOGDOC) || docType.equals("B")
    }

    def isTweet(docType: String): Boolean = {
      docType.equals(TWITTERDOC) || docType.equals("T") || docType.equals(TWEET)
    }

    def isPAN(docType: String): Boolean = {
      docType.equals(PANDOC) || docType.equals("P") || docType.equals("PAN")
    }

  }

  object DatasetParameters {

    val articleMinTextLength = 500
    val blogMinTextLength = 500
    val tweetMinTextLength = 50


    val articleTextLengthTileUpper = 95f
    val articleTextLengthTileLower = 5f

    val blogTextLengthTileUpper = 95f
    val blogTextLengthTileLower = 5f

    val tweetTextLengthTileUpper = 97f
    val tweetTextLengthTileLower = 3f


    val authorDocSizeTileUpper = 97f
    val authorDocSizeTileLower = 3f

    val authorMinDocSize = 20
    val authorMinTweetSize = 75

    val authorMaxDocSize = 5
    val authorMaxTweetSize = 25

    val articleDistinctiveTileUpper = 95f
    val articleDistinctiveTileLower = 5f

    val blogDistinctiveTileUpper = 95f
    val blogDistinctiveTileLower = 5f

    val tweetDistinctiveTileUpper = 98f
    val tweetDistinctiveTileLower = 15f

    val articleAuthorDocSizeTileUpper = 95f
    val articleAuthorDocSizeTileLower = 5f

    val blogAuthorDocSizeTileUpper = 95f
    val blogAuthorDocSizeTileLower = 5f

    val tweetAuthorDocSizeTileUpper = 99f
    val tweetAuthorDocSizeTileLower = 1f


    def append(printWriter: PrintWriter): Unit = {
      printWriter.append("===Dataset Sampling Parameters===\n")
      printWriter.append(s"ArticleTextLengthQuantileUpper: $articleTextLengthTileUpper\n")
      printWriter.append(s"ArticleTextLengthQuantileLower: $articleTextLengthTileLower\n")

      printWriter.append(s"BlogTextLengthQuantileUpper: $blogTextLengthTileUpper\n")
      printWriter.append(s"BlogTextLengthQuantileLower: $blogTextLengthTileLower\n")

      printWriter.append(s"TweetTextLengthQuantileUpper: $tweetTextLengthTileUpper\n")
      printWriter.append(s"TweetTextLengthQuantileLower: $tweetTextLengthTileLower\n")

      printWriter.append(s"authorDocSizeQuantileUpper: ${authorDocSizeTileUpper}\n")
      printWriter.append(s"authorDocSizeQuantileLower: ${authorDocSizeTileLower}\n")

      printWriter.append(s"authorMinDocSize: ${authorMinDocSize}\n")
      printWriter.append(s"authorMinTweetSize: ${authorMinTweetSize}\n")

      printWriter.append(s"authorMaxDocSize: ${authorMaxDocSize}\n")
      printWriter.append(s"authorMaxTweetSize: ${authorMaxTweetSize}\n")

      printWriter.append(s"articleDistinctiveTileUpper: ${articleDistinctiveTileUpper}\n")
      printWriter.append(s"articleDistinctiveTileLower: ${articleDistinctiveTileLower}\n")

      printWriter.append(s"blogDistinctiveTileUpper: ${blogDistinctiveTileUpper}\n")
      printWriter.append(s"blogDistinctiveTileLower: ${blogDistinctiveTileLower}\n")

      printWriter.append(s"tweetDistinctiveTileUpper: ${tweetDistinctiveTileUpper}\n")
      printWriter.append(s"tweetDistinctiveTileLower: ${tweetDistinctiveTileLower}\n")

      printWriter.append(s"articleAuthorDocSizeTileUpper: ${articleAuthorDocSizeTileUpper}\n")
      printWriter.append(s"articleAuthorDocSizeTileLower: ${articleAuthorDocSizeTileLower}\n")

      printWriter.append(s"blogAuthorDocSizeTileUpper: ${blogAuthorDocSizeTileUpper}\n")
      printWriter.append(s"blogAuthorDocSizeTileLower: ${blogAuthorDocSizeTileLower}\n")

      printWriter.append(s"tweetAuthorDocSizeTileUpper: ${tweetAuthorDocSizeTileUpper}\n")
      printWriter.append(s"tweetAuthorDocSizeTileLower: ${tweetAuthorDocSizeTileLower}\n")



    }

  }

  object DatasetResources {
    val datasetDir = Global.binary + "datasets/"
    val processedDFDir = datasetDir + "processedDF/"
    val sampledDFDir = datasetDir + "sampledDF/"
    val sentenceDFDir = datasetDir + "sentencesDF/"
    val sentenceLargeDFDir = datasetDir + "sentencesLargeDF/"
    val sampledRDDDir = datasetDir + "sampledRDD/"
    val wekaARFFDir = datasetDir + "wekaARFF/"


    val allDatasetSource = Global.resources + "binary/datasets/all"
    val allDatasetDFSource = Global.resources + "binary/datasets/all-DF"
    val panDatasetSmallSource = Global.binary + "datasets/pan-small"
    val panDatasetLargeSource = Global.binary + "datasets/pan-large"

    val hardDatasetSource = Global.resources + "binary/datasets/hard/"
    val softDatasetSource = Global.resources + "binary/datasets/soft/"
    val mediumDatasetSource = Global.resources + "binary/datasets/medium/"
    val allSampledSource = Global.resources + "binary/datasets/all-sampled"


    val statsMedium = Global.datasets + "medium-dataset.stats"
    val statsSoft = Global.datasets + "soft-dataset.stats"
    val statsHard = Global.datasets + "hard-dataset.stats"
    val statsAll = Global.datasets + "all-dataset.stats"

    val hardDataFrame = Global.dataframes + "hard"
    val softDataFrame = Global.dataframes + "soft"
    val mediumDataFrame = Global.dataframes + "medium"


    /*def prepare(): Unit = {
      delete(hardDatasetSource)
      delete(softDatasetSource)
      delete(mediumDatasetSource)
    }

    def prepareMedium(): Unit = {
      delete(mediumDatasetSource)
    }*/

   /* def delete(path: String): Unit = {
      val npath = Path(path)
      if (npath.exists) {
        Try(npath.deleteRecursively())
      }
    }*/
  }

}

object PipelineResources {
  val pipelines = Global.binary + "pipelines/"
  val executorModel = pipelines + "executor.bin"
}

object DictionaryCategory {
  val farmokoloji = "FARMOKOLOJI"
  val stopwords = "STOPWORDS"
  val phrasels = "PHRASELS"

}


object Dictionary {
  val dictionary = Global.resources + "dictionary/"
  val dictionaryEmoticons = dictionary + "emoticons/emoticons.txt"
  val dictionaryENStopWords = dictionary + "english/stopwords/stopwords.txt"
  val dictionaryENSlangWords = dictionary + "english/internet-slang/internet-slang.txt"
  val dictionaryTweetTerms = dictionary + "english/twitter/twitter.txt"
  val dictionaryTRFarmokolojiTerms = dictionary + "turkish/terms/pharmacology.txt"
  val dictionaryTRPhrasels = dictionary + "turkish/phrasels/phrasels.txt"
  val dictionaryTRPhraselsTypes = dictionary + "turkish/phrasels/phrasel-categories.txt"

  val dictionaryTRFunctionWords = dictionary + "turkish/stopwords/stopwords.txt"
  val dictionaryTRCommonPOSTags = dictionary + "turkish/postags/postags.txt"




  def loadEmoticons(): Map[String, String] = {
    val text = Source.fromFile(dictionaryEmoticons).mkString
    var map: Map[String, String] = Map()
    text.split("\n").foreach(line => {
      val split = line.split("\t+")
      val emoticonList = split(0).split("\\s+")
      val emoticonMeaning = split(1).split("\\,(\\s+?)")

      emoticonList.foreach(emo => {
        map = map.+((emo, emoticonMeaning(0)))
      })
    })

    map
  }

  def loadTerms(locale: Locale, filename: String, termCategory: String): Map[String, String] = {
    val lines = Source.fromFile(filename).getLines()
    var map = Map[String, String]()
    lines.foreach(line => {
      if (!line.trim.isEmpty) {
        val term = line.toLowerCase(locale).trim
        map = map + (term -> termCategory)
      }
    })
    map
  }

  def loadTerms(locale: Locale, filename: String): Map[String, String] = {
    val lines = Source.fromFile(filename).getLines()
    var map = Map[String, String]()
    lines.foreach(line => {
      if (!line.trim.isEmpty) {
        val termLine = line.toLowerCase(locale).trim.split("\\t+")
        val term = termLine(0).trim
        val category = termLine(1).trim
        map = map + (term -> category)
      }
    })
    map
  }

  def loadTRFarmokoliTerms(): Map[String, String] = {
    loadTerms(Parameters.trLocale, dictionaryTRFarmokolojiTerms, DictionaryCategory.farmokoloji)
  }

  def loadTRPhrasels(): Map[String, String] = {
    loadTerms(Parameters.trLocale, dictionaryTRPhrasels, DictionaryCategory.phrasels)
  }

  def loadTRPhraselsTypes(): Map[String, String] = {
    loadTerms(Parameters.trLocale, dictionaryTRPhraselsTypes)
  }

  def loadTRFunctionWords(): Map[String, String] = {
    loadTerms(Parameters.trLocale, dictionaryTRFunctionWords, DictionaryCategory.stopwords)
  }

  def loadENFunctionWords(): Map[String, String] = {
    loadTerms(Parameters.enLocale, dictionaryENStopWords, DictionaryCategory.stopwords)
  }

  def loadTRCommonPosTags(): Map[String, String] = {
    loadTerms(Parameters.trLocale, dictionaryTRCommonPOSTags)
  }

  /**
    * Independent from language
    */

  def loadFunctionWords():Map[String,String]={
    if(Parameters.turkishText) loadTRFunctionWords()
    else loadENFunctionWords()
  }

  /**
    * En not implemented yet
    *
    * @return
    */
  def loadCommonPosTags():Map[String,String] = {
    if(Parameters.turkishText) loadTRCommonPosTags()
    else Map[String,String]()
  }

  def loadPhrasels() :Map[String, String]={
    if(Parameters.turkishText) loadTRPhrasels()
    else Map[String, String]()
  }

  def loadPhraselTypes():Map[String,String] = {
    if(Parameters.turkishText) loadTRPhraselsTypes()
    else Map[String,String]()
  }

}


object TestResources {
  def main(args: Array[String]) {

    Dictionary.loadTRPhraselsTypes()
  }

}