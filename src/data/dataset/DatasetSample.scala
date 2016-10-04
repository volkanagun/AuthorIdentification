package data.dataset

import data.document.Document
import options.ParamsSample
import org.apache.spark.ml.pipes.LoaderPipe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 27.04.2016.
  */
object DatasetSample {
  //Sample by parameters : a kind of pipeline
  //Sample RDD's

  def sampleByRate(rdd: RDD[Document]): RDD[Document] = {
    if (ParamsSample.sampleByRate)
      rdd.sample(false, ParamsSample.sampleRate)
    else rdd
  }

  def sampleByRate(dataFrame: DataFrame): DataFrame = {
    if (ParamsSample.sampleByRate) {
      dataFrame.sample(false, ParamsSample.sampleRate)
    }
    else{
      dataFrame
    }
  }


  def sampleByAuthors(rdd: RDD[Document]): RDD[Document] = {
    if(ParamsSample.sampleByAuthor) {
      val authors = ParamsSample.sampleAuthorCategory.split("\\|")
      rdd.filter(document => {
        authors.contains(document.author)
      })
    }
    else{
      rdd
    }
  }

  def sampleByAuthors(df: DataFrame): DataFrame = {
    if(ParamsSample.sampleByAuthor) {
      val authors = ParamsSample.sampleAuthorCategory.split("\\|")
      df.filter(df(LoaderPipe.authorCol).isin(authors:_*))
    }
    else{
      df
    }
  }

  def sampleByGenre(rdd: RDD[Document]): RDD[Document] = {
    if(ParamsSample.sampleByGenre) {
      val genres = ParamsSample.sampleGenreCategory.split("\\|")
      rdd.filter(document => {
        genres.contains(document.genre)
      })
    }
    else{
      rdd
    }
  }

  def sampleByGenre(df: DataFrame): DataFrame = {
    if(ParamsSample.sampleByGenre) {
      val genres = ParamsSample.sampleGenreCategory.split("\\|")
      df.filter(df(LoaderPipe.genreCol).isin(genres:_*))
    }
    else{
      df
    }
  }

  def sampleByDocType(df: DataFrame): DataFrame = {
    if(ParamsSample.sampleByDocType) {
      val docTypes = ParamsSample.sampleDocTypeCategory.split("\\|")
      df.filter(df(LoaderPipe.doctypeCol).isin(docTypes:_*))
    }
    else{
      df
    }
  }
  def sampleByDocType(rdd: RDD[Document]): RDD[Document] = {
    if(ParamsSample.sampleByDocType) {
      val docTypes = ParamsSample.sampleDocTypeCategory.split("\\|")
      rdd.filter(doc=>docTypes.contains(doc.doctype))
    }
    else{
      rdd
    }
  }


  def sample(rdd: RDD[Document]): RDD[Document] = {
    if (ParamsSample.sampleAll) rdd
    else {
      val authorRDD = sampleByAuthors(rdd)
      val genreRDD = sampleByGenre(authorRDD)
      val rateRDD = sampleByRate(genreRDD)
      rateRDD
    }
  }

  def sample(df: DataFrame): DataFrame = {
    if (ParamsSample.sampleAll) df
    else {
      val authorDF = sampleByAuthors(df)
      val typeDF = sampleByDocType(authorDF)
      val genreDF = sampleByGenre(typeDF)
      val rateDF = sampleByRate(genreDF)
      rateDF
    }
  }

  def sampleNonRate(rdd:RDD[Document])={
    if (ParamsSample.sampleAll) rdd
    else {
      val authorRDD = sampleByAuthors(rdd)
      val typeRDD = sampleByDocType(authorRDD)
      val genreRDD = sampleByGenre(typeRDD)
      genreRDD
    }
  }

  def sampleNonRate(df: DataFrame): DataFrame = {
    if (ParamsSample.sampleAll) df
    else {
      val authorDF = sampleByAuthors(df)
      val typeDF = sampleByDocType(authorDF)
      val genreDF = sampleByGenre(typeDF)
      genreDF
    }
  }

}
