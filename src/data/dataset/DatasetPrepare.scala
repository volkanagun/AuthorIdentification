package data.dataset

import java.util.logging.{Level, Logger}

import data.document.Document
import language.tokenization.MyTokenizer
import options.{ParamsSample, Resources}
import options.Resources.{DatasetResources, DocumentModel, DocumentResources}
import org.apache.spark.ml.pipes.LoaderPipe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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

  def loadFlatDF(sc: SparkContext, rdd:RDD[Document]):DataFrame={
    val sql = new SQLContext(sc)
    new LoaderPipe().loadFlat(sql, rdd)
  }

  def panSmallDataset(sc: SparkContext): RDD[Document] = {
    val rdd = load(sc, DocumentResources.panSmallXMLs)
    DatasetUtil.panDataset(rdd)
  }

  def panLargeDataset(sc: SparkContext): RDD[Document] = {
    val rdd = load(sc, DocumentResources.panLargeXMLs)
    DatasetUtil.panDataset(rdd)
  }

  def allDataset(sc: SparkContext): RDD[Document] = {
    if (allDatasetExists()) loadObject(sc, DatasetResources.allDatasetSource)
    else {
      val rdd = load(sc)
      rdd.saveAsObjectFile(DatasetResources.allDatasetSource)
      rdd
    }
  }

  def allDatasetDF(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    if (DatasetLoader.exists(DatasetResources.allDatasetDFSource)) {
      sqlContext.read.load(DatasetResources.allDatasetDFSource)
    }
    else {
      val rdd = allDataset(sc)
      val df = new LoaderPipe().loadFlat(sqlContext, rdd)
      df.write.save(DatasetResources.allDatasetDFSource)
      df
    }
  }


  def hardDataset(sc: SparkContext): RDD[Document] = {
    //less distinct words
    //less text size
    //less author samples
    val rdd = allDataset(sc)
    hardDataset(rdd)
  }

  def hardDataset(rdd: RDD[Document]): RDD[Document] = {
    val shortRdd = QuantileByMeasure.applyShortLength(rdd)
    val poorRdd = QuantileByMeasure.applyPoor(shortRdd)
    val lrdd = BelowMaximumDocSize.apply(poorRdd)
    lrdd
  }

  def hardDataset(df: DataFrame): DataFrame = {
    val shortDF = QuantileByMeasure.applyShortLength(df)
    val poorDF = QuantileByMeasure.applyPoor(shortDF)
    val lrdd = BelowMaximumDocSize.apply(poorDF)
    lrdd
  }

  def softDataset(sc: SparkContext): RDD[Document] = {
    val rdd = allDataset(sc)
    softDataset(rdd)
  }

  def softDataset(rdd: RDD[Document]): RDD[Document] = {
    val longRdd = QuantileByMeasure.applyLong(rdd)
    val richRdd = QuantileByMeasure.applyRich(longRdd)
    val upperRdd = AboveMinimumDocSize.apply(richRdd)
    upperRdd
  }

  def softDataset(df: DataFrame): DataFrame = {
    val longRdd = QuantileByMeasure.applyLong(df)
    val richRdd = QuantileByMeasure.applyRich(longRdd)
    val upperRdd = AboveMinimumDocSize.apply(richRdd)
    upperRdd
  }

  def mediumDataset(sc: SparkContext, selective: Boolean = false): RDD[Document] = {
    val rdd = allDataset(sc)
    mediumDataset(rdd)
  }

  def mediumDataset(rdd: RDD[Document]): RDD[Document] = {
    val longRdd = QuantileByMeasure.applyMediumLength(rdd)
    val richRdd = QuantileByMeasure.applyAverageDistinctive(longRdd)
    val upperRdd = AboveMinimumDocSize.apply(richRdd)
    upperRdd
  }

  def mediumDataset(df: DataFrame): DataFrame = {
    val longDF = QuantileByMeasure.applyMediumLength(df)
    val richDF = QuantileByMeasure.applyAverageDistinctive(longDF)
    val upperRdd = AboveMinimumDocSize.apply(richDF)
    upperRdd
  }

  def minMediumDataset(df:DataFrame):DataFrame={

    val longDF = QuantileByMeasure.applyMediumLength(df)
    val richDF = QuantileByMeasure.applyAverageDistinctive(longDF)
    val upDF = AboveMinimumDocSize.apply(richDF)
    upDF
  }

  def minAuthorDocDataset(df:DataFrame):DataFrame={
    AboveMinimumDocSize.apply(df)
  }

  def allDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.allDatasetSource)
  }

  def sampledAllDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.allSampledSource)
  }

  def mediumDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.mediumDatasetSource)
  }

  def softDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.softDatasetSource)
  }

  def hardDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.hardDatasetSource)
  }

  def panSmallDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.panDatasetSmallSource)
  }

  def panLargeDatasetExists(): Boolean = {
    DatasetLoader.exists(DatasetResources.panDatasetLargeSource)
  }

  def sampled(sc: SparkContext): RDD[Document] = {
    if (ParamsSample.sampleMedium) sampledMediumDataset(sc)
    else if (ParamsSample.sampleSoft) sampledSoftDataset(sc)
    else if (ParamsSample.sampleHard) sampledHardDataset(sc)
    else if (ParamsSample.sampleAll) sampledAllDataset(sc)
    else if (ParamsSample.samplePANLarge) panLargeDataset(sc)
    else if (ParamsSample.samplePANSmall) panSmallDataset(sc)
    else sc.emptyRDD[Document]
  }

  def sampled(sc: SparkContext, filename: String): RDD[Document] = {
    if (DatasetLoader.exists(filename)) {
      DatasetLoader.loadObjectDocs(sc, filename)
    }
    else {
      val finalRDD = if (ParamsSample.sampleAll) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sample(rdd)
        sampled
      }
      else if (ParamsSample.sampleMedium) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sample(rdd)
        mediumDataset(sampled)
      }
      else if (ParamsSample.sampleSoft) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sample(rdd)
        softDataset(sampled)
      }
      else if (ParamsSample.sampleHard) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sample(rdd)
        hardDataset(sampled)
      }
      else if (ParamsSample.samplePANLarge || ParamsSample.samplePANSmall) {
        val panRDD = load(sc, filename)
        DatasetUtil.panDataset(panRDD)
      }
      else sc.emptyRDD[Document]

      finalRDD.saveAsObjectFile(filename)
      finalRDD
    }
  }

  def sampledNonRate(sc: SparkContext, filename: String): RDD[Document] = {
    if (DatasetLoader.exists(filename)) {
      DatasetLoader.loadObjectDocs(sc, filename)
    }
    else {
      val finalRDD = if (ParamsSample.sampleAll) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sample(rdd)
        sampled
      }
      else if (ParamsSample.sampleMedium) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sampleNonRate(rdd)
        mediumDataset(sampled)
      }
      else if (ParamsSample.sampleSoft) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sampleNonRate(rdd)
        softDataset(sampled)
      }
      else if (ParamsSample.sampleHard) {
        val rdd = allDataset(sc)
        val sampled = DatasetSample.sampleNonRate(rdd)
        hardDataset(sampled)
      }
      else if (ParamsSample.samplePANLarge || ParamsSample.samplePANSmall) {
        val panRDD = load(sc, filename)
        DatasetUtil.panDataset(panRDD)
      }
      else sc.emptyRDD[Document]

      val ratedRDD = DatasetSample.sampleByRate(finalRDD)
      ratedRDD.saveAsObjectFile(filename)
      ratedRDD
    }
  }

  def sampledFlatDF(sc: SparkContext, filename: String): DataFrame = {
    if (DatasetLoader.exists(filename)) {
      DatasetLoader.loadDFDocs(sc, filename)
    }
    else {
      val df = allDatasetDF(sc)
      val finalDF = if (ParamsSample.sampleAll) {
        val sampled = DatasetSample.sampleNonRate(df)
        sampled
      }
      else if(ParamsSample.sampleMinDocSize){
        val sampled = DatasetSample.sampleNonRate(df)
        minAuthorDocDataset(sampled)

      }
      else if (ParamsSample.sampleMinMedium) {
        val sampled = DatasetSample.sampleNonRate(df)
        minMediumDataset(sampled)
      }
      else if (ParamsSample.sampleMedium) {
        val sampled = DatasetSample.sampleNonRate(df)
        mediumDataset(sampled)
      }
      else if (ParamsSample.sampleSoft) {
        val sampled = DatasetSample.sampleNonRate(df)
        softDataset(sampled)
      }
      else if (ParamsSample.sampleHard) {
        val sampled = DatasetSample.sampleNonRate(df)
        hardDataset(sampled)
      }
      else /*if (ParamsSample.samplePANLarge || ParamsSample.samplePANSmall)*/ {
        val panRDD = load(sc, filename)
        val rdd = DatasetUtil.panDataset(panRDD)
        loadFlatDF(sc, rdd)
      }


      val ratedRDD = DatasetSample.sampleByRate(finalDF)
      ratedRDD.write.save(filename)
      ratedRDD
    }
  }

  def sampledMediumDataset(sc: SparkContext): RDD[Document] = {
    if (mediumDatasetExists()) DatasetLoader.loadObjectDocs(sc, DatasetResources.mediumDatasetSource)
    else {
      val rdd = allDataset(sc)
      val sampled = DatasetSample.sample(rdd)
      val medRDD = mediumDataset(sampled)
      medRDD.saveAsObjectFile(DatasetResources.mediumDatasetSource)
      medRDD
    }
  }

  def sampledHardDataset(sc: SparkContext): RDD[Document] = {
    if (hardDatasetExists()) DatasetLoader.loadObjectDocs(sc, DatasetResources.hardDatasetSource)
    else {
      val rdd = load(sc)
      val sampled = DatasetSample.sample(rdd)
      val harRDD = hardDataset(sampled)
      harRDD.saveAsObjectFile(DatasetResources.hardDatasetSource)
      harRDD
    }
  }

  def sampledSoftDataset(sc: SparkContext): RDD[Document] = {
    if (softDatasetExists()) DatasetLoader.loadObjectDocs(sc, DatasetResources.softDatasetSource)
    else {
      val rdd = load(sc)
      val sampled = DatasetSample.sample(rdd)
      val sofRDD = softDataset(sampled)
      sofRDD.saveAsObjectFile(DatasetResources.softDatasetSource)
      sofRDD
    }
  }

  def sampledAllDataset(sc: SparkContext): RDD[Document] = {
    if (sampledAllDatasetExists()) DatasetLoader.loadObjectDocs(sc, DatasetResources.allSampledSource)
    else {
      val rdd = load(sc)
      DatasetSample.sample(rdd)
    }
  }

  def prepareByDifficulty(sc: SparkContext): Unit = {
    val rdd = load(sc)
    hardDataset(rdd)
    softDataset(rdd)
    mediumDataset(rdd)
  }


}

object QuantileByMeasure {
  val myTokenizer = new MyTokenizer
  //Basic sample size measure
  val funcTextLength = (d: Document) => (d, d.getText().length)
  val funcDFTextLength = (d: Row) => d.getAs[String](LoaderPipe.textCol).length
  //Basic vocabulary richness measure
  val funcWordDistinctive = (d: Document) => (d, myTokenizer.tokenize(d.getText()).distinct.length)
  val funcDFWordDistinctive = (d: Row) => d.getAs[Seq[String]](LoaderPipe.tokensCol).distinct.length


  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantiles of document length">

  def contentLengthArticles(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.articleTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.articleTextLengthTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByBetweenPercentile(frdd, funcTextLength, aboveTile, belowTile)
    else frdd
  }

  def contentLengthArticles(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterArticles(df)
    val aboveTile = Resources.DatasetParameters.articleTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.articleTextLengthTileLower
    println("Article length by percentile")
    DatasetFilter.filterDocsByLengthPercentile(frdd, LoaderPipe.textCol, funcDFTextLength, aboveTile, belowTile)
  }



  def contentLengthArticlesShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetParameters.articleTextLengthTileLower
    val belowRDD = DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)
    belowRDD
  }

  def contentLengthArticlesShort(df: DataFrame): DataFrame = {
    val fdf = DatasetFilter.filterTweets(df)
    val belowTile = Resources.DatasetParameters.articleTextLengthTileLower
    val belowRDD = DatasetFilter.filterDocsByLengthPercentile(fdf, LoaderPipe.textCol, funcDFTextLength, belowTile, false)
    belowRDD
  }

  def contentLengthArticlesLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.articleTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)
  }

  def contentLengthArticlesLong(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterArticles(df)
    val aboveTile = Resources.DatasetParameters.articleTextLengthTileUpper
    DatasetFilter.filterDocsByLengthPercentile(frdd,LoaderPipe.textCol, funcDFTextLength, aboveTile, true)
  }

  def contentMinLengthArticles(df:DataFrame):DataFrame={
    val frdd = DatasetFilter.filterArticles(df)
    val minLength  = Resources.DatasetParameters.articleMinTextLength
    DatasetFilter.filterDocsByMinTextLength(frdd,LoaderPipe.textCol, minLength)
  }

  def contentLengthBlogs(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetParameters.blogTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.blogTextLengthTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByBetweenPercentile(frdd, funcTextLength, aboveTile, belowTile)
    else frdd
  }

  def contentLengthBlogs(df: DataFrame): DataFrame = {
    val fdf = DatasetFilter.filterBlogs(df)
    val aboveTile = Resources.DatasetParameters.blogTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.blogTextLengthTileLower
    println("Blog length by percentile")
    DatasetFilter.filterDocsByLengthPercentile(fdf,LoaderPipe.textCol, funcDFTextLength, aboveTile, belowTile)
  }

  def contentLengthBlogsLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetParameters.blogTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)
  }

  def contentLengthBlogsLong(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterBlogs(df)
    val aboveTile = Resources.DatasetParameters.blogTextLengthTileUpper
    DatasetFilter.filterDocsByLengthPercentile(frdd,LoaderPipe.textCol, funcDFTextLength, aboveTile, true)
  }

  def contentLengthBlogsShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val belowTile = Resources.DatasetParameters.blogTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)
  }

  def contentLengthBlogsShort(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterBlogs(df)
    val belowTile = Resources.DatasetParameters.blogTextLengthTileLower
    DatasetFilter.filterDocsByLengthPercentile(frdd,LoaderPipe.textCol, funcDFTextLength, belowTile, false)
  }

  def contentMinLengthBlogs(df:DataFrame):DataFrame={
    val frdd = DatasetFilter.filterBlogs(df)
    val minLength  = Resources.DatasetParameters.blogMinTextLength
    DatasetFilter.filterDocsByMinTextLength(frdd,LoaderPipe.textCol, minLength)
  }

  def contentLengthTweets(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower

    DatasetFilter.filterDocsByBetweenPercentile(frdd, funcTextLength, aboveTile, belowTile)
  }

  def contentLengthTweets(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower
    println("Tweet length by percentile")
    DatasetFilter.filterDocsByLengthPercentile(frdd, LoaderPipe.textCol, funcDFTextLength, aboveTile, belowTile)
  }

  def contentLengthTweetsLong(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, aboveTile, true)
  }

  def contentLengthTweetsLong(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    DatasetFilter.filterDocsByLengthPercentile(frdd, LoaderPipe.textCol, funcDFTextLength, aboveTile, true)
  }

  def contentLengthTweetsShort(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcTextLength, belowTile, false)
  }

  def contentLengthTweetsShort(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower
    DatasetFilter.filterDocsByLengthPercentile(frdd,LoaderPipe.textCol, funcDFTextLength, belowTile, false)
  }

  def contentMinLengthTweets(df:DataFrame):DataFrame={
    val frdd = DatasetFilter.filterTweets(df)
    val minLength  = Resources.DatasetParameters.tweetMinTextLength
    DatasetFilter.filterDocsByMinTextLength(frdd,LoaderPipe.textCol, minLength)
  }

  def applyMinCondition(df:DataFrame):DataFrame={
    val adf = contentMinLengthArticles(df)
    val bdf = contentMinLengthBlogs(df)
    val tdf = contentMinLengthTweets(df)

    adf.unionAll(bdf).unionAll(tdf)
  }

  def applyLong(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticlesLong(rdd)
    val brdd = contentLengthBlogsLong(rdd)
    val trdd = contentLengthTweetsLong(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyLong(df: DataFrame): DataFrame = {
    val ardd = contentLengthArticlesLong(df)
    val brdd = contentLengthBlogsLong(df)
    val trdd = contentLengthTweetsLong(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }



  def applyMediumLength(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticles(rdd)
    val brdd = contentLengthBlogs(rdd)
    val trdd = contentLengthTweets(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyMediumLength(df: DataFrame): DataFrame = {
    val ardd = contentLengthArticles(df)
    val brdd = contentLengthBlogs(df)
    val trdd = contentLengthTweets(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }

  def applyShortLength(rdd: RDD[Document]): RDD[Document] = {
    val ardd = contentLengthArticlesShort(rdd)
    val brdd = contentLengthBlogsShort(rdd)
    val trdd = contentLengthTweetsShort(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyShortLength(df: DataFrame): DataFrame = {
    val ardd = contentLengthArticlesShort(df)
    val brdd = contentLengthBlogsShort(df)
    val trdd = contentLengthTweetsShort(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantiles of word distinctiveness size">

  def distinctiveArticles(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.articleDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.articleDistinctiveTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByBetweenPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
    else
      frdd;
  }

  def distinctiveArticles(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterArticles(df)
    val aboveTile = Resources.DatasetParameters.articleDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.articleDistinctiveTileLower
    println("Article token size by percentile")
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, belowTile)

  }

  def distinctiveArticlesRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.articleDistinctiveTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveArticlesRich(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterArticles(df)
    val aboveTile = Resources.DatasetParameters.articleDistinctiveTileUpper
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, true)
  }

  def distinctiveArticlesPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterArticles(rdd)
    val belowTile = Resources.DatasetParameters.articleDistinctiveTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }

  def distinctiveArticlesPoor(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterArticles(df)
    val belowTile = Resources.DatasetParameters.articleDistinctiveTileLower
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, belowTile, false)
  }

  def distinctiveBlogs(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetParameters.blogDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.blogDistinctiveTileLower
    if (frdd.count() > 0)
      DatasetFilter.filterDocsByBetweenPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
    else frdd
  }

  def distinctiveBlogs(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterBlogs(df)
    val aboveTile = Resources.DatasetParameters.blogDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.blogDistinctiveTileLower
    println("Blog token size percentile")
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, belowTile)
  }

  def distinctiveBlogsRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val aboveTile = Resources.DatasetParameters.blogDistinctiveTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveBlogsRich(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterBlogs(df)
    val aboveTile = Resources.DatasetParameters.blogDistinctiveTileUpper
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, true)
  }

  def distinctiveBlogsPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterBlogs(rdd)
    val belowTile = Resources.DatasetParameters.blogDistinctiveTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }
  def distinctiveBlogsPoor(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterBlogs(df)
    val belowTile = Resources.DatasetParameters.blogDistinctiveTileLower
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, belowTile, false)
  }

  def distinctiveTweets(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetParameters.tweetDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.tweetDistinctiveTileLower
    DatasetFilter.filterDocsByBetweenPercentile(frdd, funcWordDistinctive, aboveTile, belowTile)
  }

  def distinctiveTweets(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val aboveTile = Resources.DatasetParameters.tweetDistinctiveTileUpper
    val belowTile = Resources.DatasetParameters.tweetDistinctiveTileLower
    println("Tweet token size percentile")
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, belowTile)
  }

  def distinctiveTweetsRich(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, aboveTile, true)
  }

  def distinctiveTweetsRich(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val aboveTile = Resources.DatasetParameters.tweetTextLengthTileUpper
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, aboveTile, true)
  }

  def distinctiveTweetsPoor(rdd: RDD[Document]): RDD[Document] = {
    val frdd = DatasetFilter.filterTweets(rdd)
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower
    DatasetFilter.filterDocsByPercentile(frdd, funcWordDistinctive, belowTile, false)
  }

  def distinctiveTweetsPoor(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val belowTile = Resources.DatasetParameters.tweetTextLengthTileLower
    DatasetFilter.filterDocsBySizePercentile(frdd,LoaderPipe.tokensCol, funcDFWordDistinctive, belowTile, false)
  }


  def applyRich(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticlesRich(rdd)
    val brdd = distinctiveBlogsRich(rdd)
    val trdd = distinctiveTweetsRich(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyRich(df: DataFrame): DataFrame = {
    val ardd = distinctiveArticlesRich(df)
    val brdd = distinctiveBlogsRich(df)
    val trdd = distinctiveTweetsRich(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }

  def applyAverageDistinctive(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticles(rdd)
    val brdd = distinctiveBlogs(rdd)
    val trdd = distinctiveTweets(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyAverageDistinctive(df: DataFrame): DataFrame = {
    val ardd = distinctiveArticles(df)
    val brdd = distinctiveBlogs(df)
    val trdd = distinctiveTweets(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }

  def applyPoor(rdd: RDD[Document]): RDD[Document] = {
    val ardd = distinctiveArticlesPoor(rdd)
    val brdd = distinctiveBlogsPoor(rdd)
    val trdd = distinctiveTweetsPoor(rdd)
    ardd.union(brdd).union(trdd)
  }

  def applyPoor(df: DataFrame): DataFrame = {
    val ardd = distinctiveArticlesPoor(df)
    val brdd = distinctiveBlogsPoor(df)
    val trdd = distinctiveTweetsPoor(df)
    ardd.unionAll(brdd).unionAll(trdd)
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Quantile Author Document Size">
  def averageArticleDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.articleAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetParameters.articleAuthorDocSizeTileLower
    DatasetFilter.filterAuthorDocsByPercentile(ardd, aboveTile, belowTile)
  }

  def averageBlogDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.blogAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetParameters.blogAuthorDocSizeTileLower
    DatasetFilter.filterAuthorDocsByPercentile(ardd, aboveTile, belowTile)
  }

  def averageTweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val ardd = DatasetFilter.filterArticles(rdd)
    val aboveTile = Resources.DatasetParameters.tweetAuthorDocSizeTileUpper
    val belowTile = Resources.DatasetParameters.tweetAuthorDocSizeTileLower
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
    val minDocSize = Resources.DatasetParameters.authorMinDocSize
    DatasetFilter.filterAuthorDocsByMinDocSize(frdd, minDocSize)
  }

  def articleBlogDocSize(df: DataFrame): DataFrame = {
    val aDF = DatasetFilter.filterArticlesOrBlogs(df)
    val minDocSize = Resources.DatasetParameters.authorMinDocSize
    DatasetFilter.filterAuthorDocsByMinDocSize(aDF, minDocSize)
  }

  def tweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isTweet()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val minDocSize = Resources.DatasetParameters.authorMinTweetSize
    DatasetFilter.filterAuthorDocsByMinDocSize(frdd, minDocSize)
  }

  def tweetDocSize(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val minDocSize = Resources.DatasetParameters.authorMinTweetSize
    DatasetFilter.filterAuthorDocsByMinDocSize(frdd, minDocSize)
  }

  def apply(rdd: RDD[Document]): RDD[Document] = {
    val docRdd = articleBlogDocSize(rdd)
    val tweetRdd = tweetDocSize(rdd)
    tweetRdd.union(docRdd)
  }

  def apply(df: DataFrame): DataFrame = {
    val docRdd = articleBlogDocSize(df)
    val tweetRdd = tweetDocSize(df)
    tweetRdd.unionAll(docRdd)
  }
}

object BelowMaximumDocSize {

  def articleBlogDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isArticle() || d.isBlog()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val maxDocSize = Resources.DatasetParameters.authorMaxDocSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(frdd, maxDocSize)
  }

  def articleBlogDocSize(df: DataFrame): DataFrame = {
    val bdf = DatasetFilter.filterBlogs(df)
    val adf = DatasetFilter.filterArticles(df)
    val ddf = adf.unionAll(bdf)
    val maxDocSize = Resources.DatasetParameters.authorMaxDocSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(ddf, maxDocSize)
  }

  def tweetDocSize(rdd: RDD[Document]): RDD[Document] = {
    val cond = (d: Document) => d.isTweet()
    val frdd = DatasetFilter.filterCondition(rdd, cond)
    val maxTweetSize = Resources.DatasetParameters.authorMaxTweetSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(frdd, maxTweetSize)
  }

  def tweetDocSize(df: DataFrame): DataFrame = {
    val frdd = DatasetFilter.filterTweets(df)
    val maxTweetSize = Resources.DatasetParameters.authorMaxTweetSize
    DatasetFilter.filterAuthorDocsByMaxDocSize(frdd, maxTweetSize)
  }

  def apply(rdd: RDD[Document]): RDD[Document] = {
    val docRdd = articleBlogDocSize(rdd)
    val tweetRdd = tweetDocSize(rdd)
    tweetRdd.union(docRdd)
  }

  def apply(df: DataFrame): DataFrame = {
    val docRdd = articleBlogDocSize(df)
    val tweetRdd = tweetDocSize(df)
    tweetRdd.unionAll(docRdd)
  }
}

object TestPrepare {


  def initLocal(): SparkContext = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("log4j.rootCategory").setLevel(Level.FINEST)
    val conf: SparkConf = new SparkConf()
      .setAppName("Stemming Parsing")
      .setMaster("local[36]")
      .set("spark.default.parallelism", "36")
      //.set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "12g")
      .set("spark.driver.maxResultSize", "24g")
      .set("spark.driver.memory", "72g")

    return new SparkContext(conf)
  }

  def modify(array:Array[Int]): Unit ={
    array(0) = 10
  }

  def main(args: Array[String]) {
    /*val sc = initLocal()
    val stats = Resources.DatasetResources.statsAll
    //DatasetResources.prepareMedium()
    //val rdd = DatasetPrepare.mediumDataset(sc, false)
    val rdd = DatasetPrepare.loadObject(sc, DatasetResources.allDatasetSource)
    DatasetStats.writeAsXML(rdd, stats)
*/

    var array = Array(1,2,3,4)
    modify(array)
    print(array(0))
  }
}

