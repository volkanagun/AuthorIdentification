package data.dataset

import java.io.{File, FileOutputStream, PrintWriter}

import data.document.Document
import org.apache.spark.ml.pipes.LoaderPipe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 06.04.2016.
  */
object DatasetStats {
  def byGenres(rdd: RDD[Document]): String = {
    var xml = "<DOC-GENRE>\n"

    val grdd = rdd.filter(d => d.getGenre() != null).groupBy(d => d.getGenre())
    grdd.map(pair => (pair._1, pair._2.toList.length))
      .collect()
      .foreach(pair => {
        xml += xmlTag(pair._1, pair._2.toString)
      })

    xml + "</DOC-GENRE>\n"
  }

  //dataframe stats should be done
  def byGenres(df: DataFrame): String = {

    var xml = "<DOC-GENRE>\n"

    df.select(LoaderPipe.genreCol)
      .groupBy(LoaderPipe.genreCol)
      .count().collect().foreach(row => {
        val genre = row.getString(0)
        val count = row.getLong(1)
          xml += xmlTag(genre,count.toString)
      })

    xml + "</DOC-GENRE>\n"

  }

  def byTypes(rdd: RDD[Document]): String = {

    var xml = "<DOC-TYPE>\n"

    val grdd = rdd.filter(d => d.getDocType() != null).groupBy(d => d.getDocType())
    grdd.map(pair => (pair._1, pair._2.toList.length))
      .collect()
      .foreach(pair => {
        xml += xmlTag(pair._1, pair._2.toString)
      })

    xml + "</DOC-TYPE>\n"
  }

  def byTypes(df: DataFrame): String = {

    var xml = "<DOC-TYPE>\n"

    df.select(LoaderPipe.doctypeCol)
      .groupBy(LoaderPipe.doctypeCol)
      .count()
      .collect()
      .foreach(row=>{
        xml += xmlTag(row.getString(0), row.getLong(1).toString)
      })

    xml + "</DOC-TYPE>\n"
  }

  def byAuthorDocSize(rdd: RDD[Document]): String = {

    var xml = "<AUTHOR-DOC>\n"
    val grdd = DatasetFilter.countDocumentsByAuthor(rdd)
    grdd.foreach(pair => {
      xml += xmlTag(pair._1, pair._2.toString)
    })

    xml + "</AUTHOR-DOC>\n"

  }

  def byAuthorDocSize(df:DataFrame):String={
    var xml = "<AUTHOR-DOC>\n"

    df.select(LoaderPipe.authorCol)
      .groupBy(LoaderPipe.authorCol)
      .count().collect().foreach(row=>{
      xml += xmlTag(row.getString(0), row.getLong(1).toString)
    })

    xml + "</AUTHOR-DOC>\n"
  }

  def byGenreAuthorDocSize(rdd: RDD[Document]): String = {

    var xml = "<AUTHOR-GENRE>\n"
    val grry = DatasetFilter.countGenresByAuthorRDD(rdd)
    grry.foreach(pair => {
      val split = pair._1.split("\\-")
      xml += xmlTag(split(0), split(1), pair._2.toString)
    })

    xml + "</AUTHOR-GENRE>\n"

  }

  def byGenreAuthorDocSize(df: DataFrame): String = {

    var xml = "<AUTHOR-GENRE>\n"

    df.select(LoaderPipe.authorCol, LoaderPipe.genreCol)
      .groupBy(LoaderPipe.authorCol, LoaderPipe.genreCol).count()
      .collect().foreach(row=>{
      xml += xmlTag(row.getString(0), row.getString(1), row.getLong(2).toString)
    })

    xml + "</AUTHOR-GENRE>\n"

  }

  def writeAsXML(rdd: RDD[Document], filename: String): Unit = {
    var xml = "<ROOT>\n"
    xml += createAsXML(rdd)
    xml += "</ROOT>"

    new PrintWriter(filename) {
      write(xml)
      close()
    }

  }

  def writeAsXML(df: DataFrame, filename: String): String = {
    var xml = "<ROOT>\n"
    xml += createAsXML(df)
    xml += "</ROOT>"



    new PrintWriter(filename) {
      write(xml)
      close()
    }

    xml
  }



  def createAsXML(rdd:RDD[Document]):String={

    var xml = byGenres(rdd)
    xml += byTypes(rdd)
    xml += byAuthorDocSize(rdd)
    xml + byGenreAuthorDocSize(rdd)

  }

  def createAsXML(df:DataFrame):String={

    var xml = byGenres(df)
    xml += byTypes(df)
    xml += byAuthorDocSize(df)
    xml + byGenreAuthorDocSize(df)

  }

  protected def xmlTag(tag: String, value: String): String = {
    "<STAT LABEL=\"" + tag + "\" VALUE=\"" + value + "\"/>\n"
  }

  protected def xmlTag(tag1: String, tag2: String, value: String): String = {
    "<STAT LABEL1=\"" + tag1 + "\" LABEL2=\"" + tag2 + "\" VALUE=\"" + value + "\"/>\n"
  }


}
