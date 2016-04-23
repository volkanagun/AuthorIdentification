package processing

import java.util.Comparator

import org.apache.spark.rdd.RDD
import processing.structures.docs.{Blog, Article, Document}

/**
  * Created by wolf on 08.01.2016.
  */
object RDDOperations extends Serializable{

  implicit val orderingBySize = new Ordering[(String,Long)]{
    override def compare(x: (String,Long), y: (String,Long)): Int = {
      if(x._2>y._2) return 1
      else if(x._2<y._2) return -1
      else 0
    }
  }
  //Group By Authors
  //Group By Genre
  //Group By Document Type

  def groupByAuthors(docRDD:RDD[Document]) : RDD[(String, Iterable[Document])]={
    docRDD.map(document=>{
      (document.getAuthor,document)
    }).groupByKey()
  }

  def groupByType(docRDD:RDD[Document]) : RDD[(String, Iterable[Document])]={
    docRDD.map(document=>{
      (document.getType,document)
    }).groupByKey()
  }

  def groupByGenre(docRDD:RDD[Document]) : RDD[(String, Iterable[Document])]={
    docRDD.map(document=>{
      if(document.isArticle) (document.asInstanceOf[Article].getGenre, document)
      else if(document.isBlog) (document.asInstanceOf[Blog].getGenre,document)
      else (Document.TWEET, document)

    }).groupByKey()
  }

  //Flatten Pairs
  def flattenDocs(docRDD:RDD[(String,Iterable[Document])]) : RDD[Document]={
    docRDD.flatMap{case(key,docs)=>{docs}}
  }

  //Several Tops
  //Top Genres
  //Top Document Types
  //Top Authors

  def docSize(docRDD:RDD[(String,Iterable[Document])]) : RDD[(String, Long)]={
    docRDD.map{case (author,doc)=>{
      (author,doc.size.toLong)
    }}
  }

  def topDocSize(n:Int, docRDD:RDD[(String, Iterable[Document])]) : RDD[(String, Iterable[Document])]={
    val sizeMap = docSize(docRDD)
    val topMap = sizeMap.top(n)(orderingBySize).toMap
    docRDD.filter{case (key,documents)=>{
      topMap.contains(key)
    }}
  }

  def authorDocSize(docRDD:RDD[Document]):RDD[(String,Long)]={
    val groupRDD = groupByAuthors(docRDD)
    docSize(groupRDD)
  }


  def genreDocSize(docRDD:RDD[Document]):RDD[(String,Long)]={
    val groupRDD = groupByGenre(docRDD)
    docSize(groupRDD)
  }


  def typeDocSize(docRDD:RDD[Document]):RDD[(String,Long)]={
    val groupRDD = groupByType(docRDD)
    docSize(groupRDD)
  }

  def topAuthorDocs(n:Int, docRDD:RDD[Document]):RDD[Document]={
    val authorDocs = groupByAuthors(docRDD)
    val topDocs = topDocSize(n,authorDocs)
    flattenDocs(topDocs)
  }

  def topGenreDocs(n:Int, docRDD:RDD[Document]):RDD[Document]={
    val genreDocs = groupByGenre(docRDD)
    val topDocs = topDocSize(n,genreDocs)
    flattenDocs(topDocs)
  }

  def topTypeDocs(n:Int, docRDD:RDD[Document]):RDD[Document]={
    val typeDocs = groupByType(docRDD)
    val topDocs = topDocSize(n,typeDocs)
    flattenDocs(topDocs)
  }


}
