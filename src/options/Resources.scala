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
    val resources = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/"
    val training = resources + "training/"
    val binary = resources + "binary/"

  }

  object OpenSentenceBD {
    val modelFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/sentences.bin"
  }

  object MorphologyResources {
    val sentenceMorphTrainLarge = Global.training + "morphology/train.merge"
    val sentenceMorphTrainDev = Global.training + "morphology/data.dev.txt"
    val sentenceMorphTest = Global.training + "morphology/test.merge"
    val crfMorphoTag = Global.binary +"crfMorphoTag.bin"

    protected  def owpl(line: String): MorphLight = {

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

    protected  def owplEnd(): MorphLight = {
      new MorphLight(".")
        .addAnalysis(".+Punc")
        .setLabel(".+Punc")
    }

    def loadMorphology(filename: String): Seq[(Int, String, Seq[MorphLight])] = {
      //load sentences
      println("Loding morphology dataset from "+filename)
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
    val modelPersonFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-person.bin"
    val modelLocationFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-location.bin"
    val modelTimeFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-time.bin"
    val modelPercentageFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-percentage.bin"
    val modelMoneyFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-money.bin"
    val modelOrganizationFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-organization.bin"
    val modelDateFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/binary/opennlp/en-ner-date.bin"

    def saveHadoop(): Unit = {

    }

    def loadHadoop(): Unit = {

    }
  }




  object LanguageModel {
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
    val articlePrefix = "article-text-"
    val blogPrefix = "blog-text-"
    val twitterPrefix = "twitter-"

    val twitterDir = Global.resources + "texts/twitter/"
    val twitterXMLs = twitterDir + twitterPrefix + "*"

    val blogDir = Global.resources + "texts/blogs/"
    val blogXMLs = blogDir + blogPrefix + "*"


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
    val TWEET = "TWEET"

    val BLOGDOC = "BLOG-DOC"

    val SPORT = "sports"
    val POLITICS = "politics"
    val TECHNOLOGY = "technology"
    val SOCIAL = "travel";
    val TWITTER = "twitter";


  }

  object DatasetPrepare {
    val articleTextLengthTileUpper = 95f
    val articleTextLengthTileLower = 5f

    val blogTextLengthTileUpper = 95f
    val blogTextLengthTileLower = 5f

    val tweetTextLengthTileUpper = 90f
    val tweetTextLengthTileLower = 10f


    val authorDocSizeTileUpper = 97f
    val authorDocSizeTileLower = 3f

    val authorMinDocSize = 5
    val authorMinTweetSize = 25

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

    val tweetAuthorDocSizeTileUpper = 98f
    val tweetAuthorDocSizeTileLower = 15f


    def saveParameters(directory: String): Unit = {
      new PrintWriter(directory + "parameters.txt") {
        write(s"ArticleTextLengthQuantileUpper=$articleTextLengthTileUpper\n")
        write(s"ArticleTextLengthQuantileLower=$articleTextLengthTileLower\n")

        write(s"BlogTextLengthQuantileUpper=$blogTextLengthTileUpper\n")
        write(s"BlogTextLengthQuantileLower=$blogTextLengthTileLower\n")

        write(s"TweetTextLengthQuantileUpper=$tweetTextLengthTileUpper\n")
        write(s"TweetTextLengthQuantileLower=$tweetTextLengthTileLower\n")

        write(s"authorDocSizeQuantileUpper=${authorDocSizeTileUpper}\n")
        write(s"authorDocSizeQuantileLower=${authorDocSizeTileLower}\n")

        write(s"authorMinDocSize=${authorMinDocSize}\n")
        write(s"authorMinTweetSize=${authorMinTweetSize}\n")

        write(s"authorMaxDocSize=${authorMaxDocSize}\n")
        write(s"authorMaxTweetSize=${authorMaxTweetSize}\n")

        close()
      }


    }

  }

  object DatasetResources {
    val hardDatasetSource = Global.resources + "binary/datasets/hard"
    val softDatasetSource = Global.resources + "binary/datasets/soft"
    val mediumDatasetSource = Global.resources + "binary/datasets/medium"

    def prepare(): Unit = {
      delete(hardDatasetSource)
      delete(softDatasetSource)
      delete(mediumDatasetSource)
    }

    def prepareMedium(): Unit = {
      delete(mediumDatasetSource)
    }

    def delete(path: String): Unit = {
      val npath = Path(path)
      if (npath.exists) {
        Try(npath.deleteRecursively())
      }
    }
  }

}

object DictionaryCategory{
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
  val dictionaryTRFarmokolojiTerms = dictionary+"turkish/terms/pharmacology.txt"
  val dictionaryTRPhrasels = dictionary+"turkish/phrasels/phrasels.txt"
  val dictionaryTRPhraselsTypes = dictionary+"turkish/phrasels/phrasel-categories.txt"

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

  def loadTerms(locale:Locale,filename:String, termCategory:String):Map[String, String]={
    val lines = Source.fromFile(filename).getLines()
    var map = Map[String,String]()
    lines.foreach(line=>{
      val term = line.toLowerCase(locale).trim
      map = map + (term->termCategory)
    })
    map
  }

  def loadTerms(locale:Locale, filename:String):Map[String,String]={
    val lines = Source.fromFile(filename).getLines()
    var map = Map[String,String]()
    lines.foreach(line=>{
      val termLine = line.toLowerCase(locale).trim.split("\\t+")
      val term = termLine(0).trim
      val category = termLine(1).trim
      map = map + (term->category)
    })
    map
  }

  def loadTRFarmokoliTerms() : Map[String,String] = {
    loadTerms(Parameters.trLocale,dictionaryTRFarmokolojiTerms,DictionaryCategory.farmokoloji)
  }

  def loadTRPhrasels() : Map[String,String] = {
    loadTerms(Parameters.trLocale, dictionaryTRPhrasels, DictionaryCategory.phrasels)
  }

  def loadTRPhraselsTypes():Map[String,String]={
    loadTerms(Parameters.trLocale, dictionaryTRPhraselsTypes)
  }

}


object TestResources {
  def main(args: Array[String]) {
    /*val sc = DocumentResources.initLocal
    DocumentResources.saveToHadoop(sc)*/
  val txt = "f&oacute;lu"
  val mtxt = txt.replaceAll("&oacute;","Ó")
    println(mtxt)
  }

}