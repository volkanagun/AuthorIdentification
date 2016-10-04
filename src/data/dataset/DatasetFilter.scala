package data.dataset

import java.util.logging.{Level, Logger}
import java.util.regex.Pattern

import data.document.Document
import language.tokenization.MyTokenizer
import options.Resources
import options.Resources.DocumentModel
import org.apache.spark.ml.pipes.LoaderPipe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}

/**
  * Created by wolf on 01.04.2016.
  */
object DatasetFilter {
  //Complex filtering options, grouping by author size, text size and etc.

  //Count
  //Count number of documents
  //Count number of authors
  //Count documents for each author, genre, document type

  def count(rdd: RDD[Document]): Long = {
    rdd.count()
  }

  def countParagraphs(rdd: RDD[Document]): Double = {
    rdd.treeAggregate(0.0)((size, doc) => {
      size + doc.getParagraphs().length
    }, (c1, c2) => {
      c1 + c2
    })
  }

  def countAuthors(rdd: RDD[Document]): Long = {
    rdd.map(document => document.getAuthor()).distinct().count()
  }

  def countGenres(rdd: RDD[Document]): Long = {
    rdd.map(document => document.getGenre()).distinct().count()
  }

  def countDocumentsByGenre(rdd: RDD[Document]): Array[(String, Int)] = {
    rdd.groupBy(document => {
      document.getGenre()
    })
      .map(pair => (pair._1, pair._2.toList.length))
      .collect()
  }

  def countDocumentsByDocType(rdd: RDD[Document]): Array[(String, Int)] = {
    rdd.groupBy(document => {
      document.getDocType()
    })
      .map(pair => (pair._1, pair._2.toList.length))
      .collect()
  }

  def countDocumentsByAuthor(rdd: RDD[Document]): Array[(String, Int)] = {
    rdd.groupBy(document => {
      document.getAuthor()
    })
      .map(pair => (pair._1, pair._2.toList.length))
      .collect()
  }

  def countDocumentsByAuthorRDD(rdd: RDD[Document]): RDD[(String, Int)] = {
    rdd.groupBy(document => {
      document.getAuthor()
    }).map(pair => (pair._1, pair._2.toList.length))
  }

  def countDocumentsByAuthorDF(df:DataFrame): DataFrame = {
    df.groupBy(LoaderPipe.authorCol).count()
  }

  def countGenresByAuthorRDD(rdd: RDD[Document]): Array[(String, Long)] = {
    val frdd = rdd.filter(document=>document.getGenre()!=null)
    frdd.map(document => {
      document.getAuthor()+"-"+document.getGenre() }).countByValue().toArray
  }

  def averageDocumentSize(rdd: RDD[Document]): Double = {
    val total = rdd.map(document => document.getText().length).reduce((a, b) => a + b)
    total.toDouble / rdd.count()
  }

  def averageTitleSize(rdd: RDD[Document]): Double = {
    val filtered = rdd.filter(document => document.getTitle() != null)
    val total = filtered.map(document => document.getTitle().length)
      .reduce((a, b) => a + b)

    total.toDouble / filtered.count()

  }

  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Averages">

  def average(total: Array[(String, Int)], countMap: Map[String, Iterable[(String, Int)]]): Array[(String, Double)] = {
    total.map(pair => {
      val num = countMap.get(pair._1).get.toList.length
      (pair._1, pair._2.toDouble / num)
    })
  }

  def averageBy(rdd: RDD[Document], func: Function1[Document, (String, Int)]): Array[(String, Double)] = {
    val myrdd = rdd.map(func).cache()
    val total = myrdd.reduceByKey((a, b) => a + b).collect()
    val countMap = myrdd.groupBy(pair => {
      pair._1
    })
      .collectAsMap()
      .toMap

    average(total, countMap)
  }

  def averageDocumentSizeByGenre(rdd: RDD[Document]): Array[(String, Double)] = {
    val func = (d: Document) => (d.getGenre(), d.getText().length)

    averageBy(rdd, func)
  }

  def averageDocumentSizeByAuthor(rdd: RDD[Document]): Array[(String, Double)] = {
    val func = (d: Document) => (d.getAuthor(), d.getText().length)

    averageBy(rdd, func)
  }

  def averageDocumentSizeByDocType(rdd: RDD[Document]): Array[(String, Double)] = {
    val func = (d: Document) => (d.getDocType(), d.getText().length)
    averageBy(rdd, func)
  }

  def averageParagraphNumByGenre(rdd: RDD[Document]): Array[(String, Double)] = {
    val func = (d: Document) => (d.getGenre(), d.getParagraphs().size)
    val myrdd = rdd.filter(document => document.getParagraphs().length > 0)
    averageBy(myrdd, func)
  }

  def averageParagraphNumByAuthor(rdd: RDD[Document]): Array[(String, Double)] = {
    val func = (d: Document) => (d.getAuthor(), d.getParagraphs().size)
    val myrdd = rdd.filter(document => document.getParagraphs().length > 0)
    averageBy(myrdd, func)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////


  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Max and Mins">

  def histogram(sc: SparkContext, array: Array[(String, Double)]): (Array[Double], Array[Long]) = {
    sc.parallelize(array).map(pair => pair._2).histogram(10)
  }

  def minSizeBy(rdd: RDD[Document], func: Function1[Document, (String, Int)]): (String, Int) = {
    val myrdd = rdd.map(func).cache()
    val ordering = new Ordering[(String, Int)]() {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        x._2.compareTo(y._2)
      }
    }

    myrdd.min()(ordering)
  }

  def maxSizeBy(rdd: RDD[Document], func: Function1[Document, (String, Int)]): (String, Int) = {
    val myrdd = rdd.map(func).cache()
    val ordering = new Ordering[(String, Int)]() {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        x._2.compareTo(y._2)
      }
    }
    myrdd.max()(ordering)
  }

  def maxParagraphNumByAuthor(rdd: RDD[Document]): (String, Int) = {
    val func = (d: Document) => {
      (d.getAuthor(), d.getParagraphs().length)
    }
    val myrdd = rdd.filter(d => d.getParagraphs().length > 0)
    maxSizeBy(myrdd, func)
  }

  def maxParagraphNumByGenre(rdd: RDD[Document]): (String, Int) = {
    val func = (d: Document) => {
      (d.getGenre(), d.getParagraphs().length)
    }

    val myrdd = rdd.filter(d => d.getParagraphs().length > 0)
    maxSizeBy(myrdd, func)
  }

  def minParagraphNumByAuthor(rdd: RDD[Document]): (String, Int) = {
    val func = (d: Document) => {
      (d.getAuthor(), d.getParagraphs().length)
    }

    val myrdd = rdd.filter(d => d.getParagraphs().length > 0)
    minSizeBy(myrdd, func)
  }

  def minParagraphNumByGenre(rdd: RDD[Document]): (String, Int) = {
    val func = (d: Document) => {
      (d.getGenre(), d.getParagraphs().length)
    }

    val myrdd = rdd.filter(d => d.getParagraphs().length > 0)
    minSizeBy(myrdd, func)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////


  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Filtering">


  def filterCondition(rdd:RDD[Document], condition: Function1[Document, Boolean]):RDD[Document]={
    rdd.filter(condition)
  }

  def filterArticles(rdd: RDD[Document]): RDD[Document] = {
    rdd.filter(d => {
      d.isArticle()
    })
  }

  def filterArticles(df: DataFrame): DataFrame = {
    df.filter(df(LoaderPipe.doctypeCol)===DocumentModel.ARTICLEDOC)
  }



  def filterBlogs(rdd: RDD[Document]): RDD[Document] = {
    rdd.filter(d => {
      d.isBlog()
    })
  }

  def filterBlogs(df: DataFrame): DataFrame = {
    df.filter(df(LoaderPipe.doctypeCol)===DocumentModel.BLOGDOC)
  }

  def filterArticlesOrBlogs(df: DataFrame): DataFrame = {
    df.filter(df(LoaderPipe.doctypeCol)===DocumentModel.BLOGDOC || df(LoaderPipe.doctypeCol)===DocumentModel.ARTICLEDOC)
  }

  def filterTweets(rdd: RDD[Document]): RDD[Document] = {
    rdd.filter(d => {
      d.isTweet()
    })
  }

  def filterTweets(df: DataFrame): DataFrame = {
    df.filter(df(LoaderPipe.doctypeCol)===DocumentModel.TWITTERDOC)
  }


  def filterByMinDistinctWords(rdd: RDD[Document], tokenizer: MyTokenizer, minDistinct: Int = 5): RDD[Document] = {
    rdd.map(d => {
      (d, tokenizer.tokenize(d.getText()).length)
    }).filter(pair => {
      pair._2 > minDistinct
    }).map(pair => pair._1)
  }

  def removeTweetsWithPatterns(rdd: RDD[Document], regexes: Array[String]): RDD[Document] = {
    rdd.map(d => {
      (d, d.getText())
    }).filter(pair => {
      !regexes.exists(regex => {
        Pattern.compile(regex).matcher(pair._2).find()
      })
    }).map(pair => pair._1)
  }

  def filterAuthorDocsByMinDocSize(rdd: RDD[Document], minDocSize: Int = 5): RDD[Document] = {
    val authorMap = countDocumentsByAuthor(rdd).filter(pair => pair._2 > minDocSize).toMap
    rdd.filter(d => {
      authorMap.contains(d.getAuthor())
    })
  }

  def filterAuthorDocsByMinDocSize(df: DataFrame, minDocSize: Int): DataFrame = {
    val authorDF = countDocumentsByAuthorDF(df)
    val authorArr = authorDF.filter(authorDF("count") >= minDocSize)
      .map(row=>row.getString(0)).collect()

    df.filter(df(LoaderPipe.authorCol).isin(authorArr:_*))

  }

  def filterAuthorDocsByMaxDocSize(rdd: RDD[Document], maxDocSize: Int = 100): RDD[Document] = {
    val authorMap = countDocumentsByAuthor(rdd).filter(pair => pair._2 < maxDocSize).toMap
    rdd.filter(d => {
      authorMap.contains(d.getAuthor())
    })
  }

  def filterAuthorDocsByMaxDocSize(df: DataFrame, maxDocSize: Int): DataFrame = {
    val authorDF = countDocumentsByAuthorDF(df)
    val authorArr = authorDF.filter(authorDF("count") < maxDocSize)
      .map(row=>row.getString(0)).collect()

    df.filter(df(LoaderPipe.authorCol).isin(authorArr:_*))

  }

  def filterAuthorDocsByPercentile(rdd: RDD[Document], tile:Double, above:Boolean=true): RDD[Document] = {
    val authorRDD = countDocumentsByAuthorRDD(rdd)

    val docSizeRDD = authorRDD.map(pair=>pair._2)
    val value = DatasetUtil.computePercentile(docSizeRDD, tile)
    val map = if(above) {
      authorRDD.filter(pair=>{pair._2>value}).collectAsMap()
    }
    else{
      authorRDD.filter(pair=>{pair._2<value}).collectAsMap()
    }

    rdd.filter(d => {
      map.contains(d.getAuthor())
    })
  }

  def filterAuthorDocsByDFPercentile(df: DataFrame, tile:Double, above:Boolean = true): DataFrame = {
    val authorDF = countDocumentsByAuthorDF(df)

    val docSizeRDD = authorDF.select("count").map(row=>row.getInt(0))
    val value = DatasetUtil.computePercentile(docSizeRDD, tile)
    val array = if(above) {
      authorDF.filter(authorDF("count") > value)
        .select(LoaderPipe.authorCol)
        .map(row=>row.getString(0))
        .collect()
    }
    else{
      authorDF.filter(authorDF("count") < value)
        .select(LoaderPipe.authorCol)
        .map(row=>row.getString(0))
        .collect()
    }

    df.filter(df(LoaderPipe.authorCol).isin(array:_*))

  }



  def filterAuthorDocsByPercentile(rdd:RDD[Document], tileAbove:Double, tileBelow:Double) : RDD[Document]={
    val rddBelow = DatasetFilter.filterAuthorDocsByPercentile(rdd,tileAbove, false)
    val frdd= DatasetFilter.filterAuthorDocsByPercentile(rdd,tileBelow, true)
    frdd.union(rddBelow)
  }

  def filterAuthorDocsByPercentile(df:DataFrame, tileAbove:Double, tileBelow:Double) : DataFrame = {
    val dfBelow = DatasetFilter.filterAuthorDocsByDFPercentile(df,tileAbove, false)
    val dfAbove= DatasetFilter.filterAuthorDocsByDFPercentile(df,tileBelow, true)
    dfAbove.unionAll(dfBelow)
  }


  def filterDocsByPercentile(rdd: RDD[Document], func: Function1[Document, (Document, Int)], tile: Double, above: Boolean = true): RDD[Document] = {
    val frdd = rdd.map(func)
    val myrdd = frdd.map(pair => pair._2)
    val value = DatasetUtil.computePercentile(myrdd, tile)
    println("Percentile value : " + value)

    if (above) frdd.filter(pair => pair._2 > value).map(pair => pair._1)
    else frdd.filter(pair => pair._2 < value).map(pair=>pair._1)
  }


   def filterDocsByBetweenPercentile(rdd: RDD[Document], func: Function1[Document, (Document, Int)], tileAbove: Double, tileBelow:Double): RDD[Document] = {
    val frdd = rdd.map(func)
    val myrdd = frdd.map(pair => pair._2)
    val aboveValue = DatasetUtil.computePercentile(myrdd, tileAbove)
    val belowValue = DatasetUtil.computePercentile(myrdd, tileBelow)
    println("Percentile above value : " + aboveValue)
    println("Percentile below value : " + belowValue)
    frdd.filter(pair => pair._2 < aboveValue && pair._2 > belowValue).map(pair => pair._1)
  }

  def filterDocsBySizePercentile(df: DataFrame, colName:String, func: Function1[Row, Int], tileAbove: Double, tileBelow:Double): DataFrame = {
    val frdd = df.select(colName).map(func)

    val aboveValue = DatasetUtil.computePercentile(frdd, tileAbove)
    val belowValue = DatasetUtil.computePercentile(frdd, tileBelow)
    println("Percentile above value : " + aboveValue)
    println("Percentile below value : " + belowValue)
    df.filter(functions.size(df(colName)) < aboveValue && functions.size(df(colName)) > belowValue)
  }

  def filterDocsByLengthPercentile(df: DataFrame, colName:String, func: Function1[Row, Int], tileAbove: Double, tileBelow:Double): DataFrame = {
    val frdd = df.select(colName).map(func)

    val aboveValue = DatasetUtil.computePercentile(frdd, tileAbove)
    val belowValue = DatasetUtil.computePercentile(frdd, tileBelow)
    println("Percentile above value : " + aboveValue)
    println("Percentile below value : " + belowValue)
    df.filter(functions.length(df(colName)) < aboveValue && functions.length(df(colName)) > belowValue)
  }



  def filterDocsByLengthPercentile(df: DataFrame, filterCol:String, func: Function1[Row, Int], tile: Double, above: Boolean = true): DataFrame = {

    val frdd = df.select(filterCol).map(func)
    val value = DatasetUtil.computePercentile(frdd, tile)
    println("Percentile value : " + value)

    if (above) df.filter(functions.length(df(filterCol)).>=(value))
    else df.filter(functions.length(df(filterCol)).<(value))
  }

  def filterDocsBySizePercentile(df: DataFrame, filterCol:String, func: Function1[Row, Int], tile: Double, above: Boolean = true): DataFrame = {

    val frdd = df.select(filterCol).map(func)
    val value = DatasetUtil.computePercentile(frdd, tile)
    println("Percentile value : " + value)

    if (above) df.filter(functions.size(df(filterCol)).>=(value))
    else df.filter(functions.size(df(filterCol)).<(value))
  }


  def filterDocsByMinTextLength(df:DataFrame, filterCol:String, minLength:Int):DataFrame={
    df.filter(functions.length(df(filterCol)) >= minLength)
  }


  def filterDocsByPercentile(df: DataFrame, filterCol:String, func: Function1[Row, Int], tileAbove: Double, tileBelow: Double): DataFrame = {
    val dfBelow = DatasetFilter.filterDocsByLengthPercentile(df,filterCol, func, tileAbove, false)
    val dfHigher= DatasetFilter.filterDocsByLengthPercentile(df,filterCol, func,tileBelow, true)
    dfHigher.intersect(dfBelow)
  }



  def filterDocsByPercentile(rdd: RDD[Document], func: Function1[Document, (Document, Int)], tile: Double): RDD[Document] = {
    val frdd = rdd.map(func)
    val myrdd = frdd.map(pair => pair._2)
    val rest = 100 - tile
    val isabove = tile > 50
    val valueAbove = DatasetUtil.computePercentile(myrdd, tile)
    val valueBelow = DatasetUtil.computePercentile(myrdd, rest)
    println("Above Percentile value : " + valueAbove)
    println("Below Percentile value : " + valueBelow)

    if(isabove) {
      frdd.filter(pair => pair._2 > valueAbove && pair._2 < valueBelow).map(pair => pair._1)
    }
    else{
      frdd.filter(pair => pair._2 < valueBelow && pair._2 > valueAbove).map(pair => pair._1)
    }
  }



  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Filter Duplicates">
  def filterDuplicateTexts(rdd:RDD[Document]):RDD[Document]={
    rdd.distinct(24)(new Ordering[Document](){
      def compare(x:Document, y:Document): Int ={
        x.getText().compareTo(y.getText())
      }
    })
  }
  //</editor-fold>
  ///////////////////////////////////////////////////////////


  //Filter what based on what by a pipeline
  //Filter authors based on document sizes
  //Filter documents based on text and/or paragraph sizes


  //Quantiles of text sizes,
}

object Test {

  val sc = initLocal

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

  def testQuantile() = {
    val aprefix = Resources.DocumentResources.articlePrefix
    val prefixes = Array("29", "30", "31").map(p => aprefix + p)
    val rdd = DatasetLoader.loadArticleDocs(sc, prefixes).filter(d => d.hasLengthGreater(0))
    val func = (d: Document) => (d, d.getText().length)
    val frdd = DatasetFilter.filterDocsByPercentile(rdd, func, 5)
    println(frdd.count() + "/" + rdd.count())
    frdd.take(100).foreach(d => {
      println(d.getText().length)
    })
  }

  def testDuplicates(): Unit ={
    val rdd = DatasetLoader.loadTweetDocs(sc)
    println(rdd.count())

  }

  def main(args: Array[String]) {
    testDuplicates()
  }

}