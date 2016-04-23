package data.dataset

import java.io.{FileOutputStream, PrintWriter}

import data.document.Document
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 06.04.2016.
  */
object DatasetStats {
  def byGenres(rdd: RDD[Document], filename:String): Unit = {
    new PrintWriter(new FileOutputStream(filename, true)) {
      write("Document Genre Sizes\n")

      val grdd = rdd.filter(d => d.getGenre() != null).groupBy(d => d.getGenre())
      grdd.map(pair => (pair._1, pair._2.toList.length))
        .collect()
        .foreach(pair => {
          write("Genre:" + pair._1 + " Size:" + pair._2+"\n")
        })
    }.close()
  }

  def byTypes(rdd: RDD[Document], filename:String): Unit = {
    new PrintWriter(new FileOutputStream(filename, true)) {
      write("Document Type Sizes\n")

      val grdd = rdd.filter(d => d.getDocType() != null).groupBy(d => d.getDocType())
      grdd.map(pair => (pair._1, pair._2.toList.length))
        .collect()
        .foreach(pair => {
          write("Type:" + pair._1 + " Size:" + pair._2+"\n")
        })
    }.close()
  }

  def byAuthorDocSize(rdd:RDD[Document], filename:String):Unit={
    new PrintWriter(new FileOutputStream(filename, true)) {
      write("Author Document Sizes\n")
      val grdd = DatasetFilter.countDocumentsByAuthor(rdd)
      grdd.foreach(pair => {
        write("Author:" + pair._1 + " Size:" + pair._2+"\n")
      })
    }.close()
  }

  def byGenreAuthorDocSize(rdd:RDD[Document], filename:String):Unit={
    new PrintWriter(new FileOutputStream(filename, true)) {
      write("Author-Genre Document Sizes\n")
      val grry = DatasetFilter.countGenresByAuthorRDD(rdd)
      grry.foreach(pair => {
        write("Author-Genre: " + pair._1 + " Size:" + pair._2+"\n")
      })
    }.close()
  }

}
