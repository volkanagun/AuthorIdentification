package data.dataset

import java.util.logging.{Level, Logger}

import data.document.Document
import language.tokenization.MyTokenizer
import options.Resources
import options.Resources.{DatasetResources, DocumentResources}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 05.04.2016.
  */
object DatasetPrepare {
  //Load all dataset filter by methods
  //Use different combination of filter methods in different pipelines

  def load(sc: SparkContext): RDD[Document] = {
    val dirs = Array(Resources.DocumentResources.articleXMLs, Resources.DocumentResources.blogXMLs, Resources.DocumentResources.twitterXMLs)
    DatasetLoader.loadDocuments(sc, dirs)
  }

  def load(sc: SparkContext, directory: String): RDD[Document] = {
    DatasetLoader.loadDocuments(sc, directory)
  }

  def loadObject(sc: SparkContext, directory: String): RDD[Document] = {
    DatasetLoader.loadObjectDocs(sc, directory)
  }




  def hardDataset(sc: SparkContext): Unit = {
    //less distinct words
    //less text size
    //less author samples
    val rdd = load(sc)
    val shortRdd = QuantileByMeasure.applyShort(rdd)
    val poorRdd = QuantileByMeasure.applyPoor(shortRdd)
    val lrdd = BelowMaximumDocSize.apply(poorRdd)
    DatasetLoader.saveObjectDocs(Resources.DatasetResources.hardDatasetSource, lrdd)
  }

  def softDataset(sc: SparkContext): Unit = {
    val rdd = load(sc)
    val longRdd = QuantileByMeasure.applyLong(rdd)
    val richRdd = QuantileByMeasure.applyRich(longRdd)
    val upperRdd = AboveMinimumDocSize.apply(richRdd)
    DatasetLoader.saveObjectDocs(Resources.DatasetResources.softDatasetSource, upperRdd)
  }

  def mediumDataset(sc: SparkContext, selective: Boolean = false): RDD[Document] = {
    val rdd = if (!selective) load(sc) else loadObject(sc, DatasetResources.mediumDatasetSource)
    println("Total number of documents: " + rdd.count())
    val longRdd = QuantileByMeasure.applyMedium(rdd)
    val richRdd = QuantileByMeasure.applyAverage(longRdd)
    val upperRdd = AboveMinimumDocSize.apply(richRdd)

    DatasetLoader.saveObjectDocs(Resources.DatasetResources.mediumDatasetSource, upperRdd)
    upperRdd
  }

  def prepareByDifficulty(sc: SparkContext): Unit = {
    hardDataset(sc)
    softDataset(sc)
    mediumDataset(sc)
  }

  def main(args: Array[String]) {

  }

}

object QuantileByMeasure {
  val myTokenizer = new MyTokenizer
  //Basic sample size measure
  val funcTextLength = (d: Document) => (d, d.getText().length)
  //Basic vocabulary richness measure
  val funcWordDistinctive = (d: Document) => (d, myTokenizer.tokenize(d.getText()).distinct.length)


  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantiles of document length">

  def contentLengthArticles(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.articleTextLengthTileUpper
    val belowTile = Resources.DatasetPrepare.articleTextLengthTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, belowTile)
    else frdd
  }

  def contentLengthArticlesShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetPrepare.articleTextLengthTileLower
    val belowRDD = DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)
    belowRDD
  }

  def contentLengthArticlesLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.articleTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)

  }

  def contentLengthBlogs(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetPrepare.blogTextLengthTileUpper
    val belowTile = Resources.DatasetPrepare.blogTextLengthTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, belowTile)
    else frdd
  }

  def contentLengthBlogsLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetPrepare.blogTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)
  }

  def contentLengthBlogsShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val belowTile = Resources.DatasetPrepare.blogTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)
  }


  def contentLengthTweets(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetPrepare.tweetTextLengthTileUpper
    val belowTile = Resources.DatasetPrepare.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, belowTile)
  }

  def contentLengthTweetsLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetPrepare.tweetTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)
  }

  def contentLengthTweetsShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetPrepare.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)

  }

  def applyLong(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticlesLong(rdd)
    val brdd = contentLengthBlogsLong(rdd)
    val trdd = contentLengthTweetsLong(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyMedium(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticles(rdd)
    val brdd = contentLengthBlogs(rdd)
    val trdd = contentLengthTweets(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyShort(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticlesShort(rdd)
    val brdd = contentLengthBlogsShort(rdd)
    val trdd = contentLengthTweetsShort(rdd)
    ardd.union(brdd).union(trdd)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantiles of word distinctiveness size">

  def distinctiveArticles(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.articleDistinctiveTileUpper
    val belowTile = Resources.DatasetPrepare.articleDistinctiveTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
    else
      frdd;
  }

  def distinctiveArticlesRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.articleDistinctiveTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveArticlesPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val belowTile = Resources.DatasetPrepare.articleDistinctiveTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }

  def distinctiveBlogs(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetPrepare.blogDistinctiveTileUpper
    val belowTile = Resources.DatasetPrepare.blogDistinctiveTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
    else frdd
  }

  def distinctiveBlogsRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetPrepare.blogDistinctiveTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveBlogsPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val belowTile = Resources.DatasetPrepare.blogDistinctiveTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }

  def distinctiveTweets(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetPrepare.tweetTextLengthTileUpper
    val belowTile = Resources.DatasetPrepare.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
  }

  def distinctiveTweetsRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetPrepare.tweetTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveTweetsPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetPrepare.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }


  def applyRich(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticlesRich(rdd)
    val brdd = distinctiveBlogsRich(rdd)
    val trdd = distinctiveTweetsRich(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyAverage(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticles(rdd)
    val brdd = distinctiveBlogs(rdd)
    val trdd = distinctiveTweets(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyPoor(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticlesPoor(rdd)
    val brdd = distinctiveBlogsPoor(rdd)
    val trdd = distinctiveTweetsPoor(rdd)
    ardd.union(brdd).union(trdd)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantile Author Document Size">
  def averageArticleDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.articleAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetPrepare.articleAuthorDocSizeTileLower
    DatasetFilter.filterAuthorDocsByPercentile(ardd, aboveTile, belowTile)
  }

  def averageBlogDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.blogAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetPrepare.blogAuthorDocSizeTileLower
    DatasetFilter.filterAuthorDocsByPercentile(ardd, aboveTile, belowTile)
  }

  def averageTweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetPrepare.tweetAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetPrepare.tweetAuthorDocSizeTileLower
    DatasetFilter.filterAuthorDocsByPercentile(ardd, aboveTile, belowTile)
  }

  def averageDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = averageArticleDocSize(rdd)
    val brdd = averageBlogDocSize(rdd)
    val trdd = averageTweetDocSize(rdd)
    ardd.union(brdd).union(trdd)
  }


  //</editor-fold>
  ///////////////////////////////////////////////////////////


}

object AboveMinimumDocSize {

  def articleBlogDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isArticle() || d.isBlog()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val minDocSize = Resources.DatasetPrepare.authorMinDocSize
    DatasetFilter.filterAuthorDocsByMinDocSize(frdd, minDocSize)
  }

  def tweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isTweet()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val minDocSize = Resources.DatasetPrepare.authorMinTweetSize
    DatasetFilter.filterAuthorDocsByMinDocSize(frdd, minDocSize)
  }

  def apply(rdd: RDD[Document]): RDD[Document] = {
    val docRdd = articleBlogDocSize(rdd)
    val tweetRdd = tweetDocSize(rdd)
    tweetRdd.union(docRdd)
  }
}

object BelowMaximumDocSize {

  def articleBlogDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isArticle() || d.isBlog()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val maxDocSize = Resources.DatasetPrepare.authorMaxDocSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(frdd, maxDocSize)
  }

  def tweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isTweet()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val maxTweetSize = Resources.DatasetPrepare.authorMaxTweetSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(frdd, maxTweetSize)
  }

  def apply(rdd: RDD[Document]): RDD[Document] = {
    val docRdd = articleBlogDocSize(rdd)
    val tweetRdd = tweetDocSize(rdd)
    tweetRdd.union(docRdd)
  }
}

object TestPrepare {


  def initLocal(): SparkContext = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("log4j.rootCategory").setLevel(Level.FINEST)
    val conf: SparkConf = new SparkConf()
      .setAppName("Stemming Parsing")
      .setMaster("local[24]")
      .set("spark.default.parallelism", "24")
      //.set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "12g")
      .set("spark.driver.maxResultSize", "24g")
      .set("spark.driver.memory", "72g")

    return new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val sc = initLocal()
    val stats = "resources/dataSet.stats"
    DatasetResources.prepareMedium()
    val rdd = DatasetPrepare.mediumDataset(sc, false)
    //val rdd = DatasetPrepare.loadObject(sc, DatasetResources.mediumDatasetSource)
    DatasetStats.byTypes(rdd, stats)
    DatasetStats.byGenres(rdd, stats)
    DatasetStats.byAuthorDocSize(rdd, stats)
    DatasetStats.byGenreAuthorDocSize(rdd, stats)

  }
}

