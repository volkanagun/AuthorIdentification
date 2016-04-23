package data.dataset

import data.document.Document
import options.Resources
import options.Resources.DocumentResources
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 01.04.2016.
  */
object DatasetLoader {
  //Read anything and map to single datastructure document
  //Mark whether it is pan, twitter, article, or blog
  //Load as RDD[Document]
  def loadFiles(sc: SparkContext, directory: String): RDD[(String, String)] = {
    sc.wholeTextFiles(directory)
  }


  def loadFiles(sc: SparkContext, directories: Array[String]): RDD[(String, String)] = {
    if (directories.isEmpty) return null;
    else {
      var unionRDD = sc.parallelize(Array[(String, String)]())

      directories.foreach(directory => {
        unionRDD = unionRDD.union(loadFiles(sc, directory))
      })

      unionRDD
    }
  }

  def loadDocuments(sc:SparkContext):RDD[Document]={
    val tweetRDD = loadDocuments(sc, DocumentResources.twitterXMLs)
    val blogRDD = loadDocuments(sc, DocumentResources.blogXMLs)
    val articleRDD = loadDocuments(sc, DocumentResources.articleXMLs)

    articleRDD.union(blogRDD).union(tweetRDD)
  }

  def loadDocuments(sc: SparkContext, directory: String, startsWith: String): RDD[Document] = {

    loadDocuments(sc, directory + startsWith + "*")

  }

  def loadDocuments(sc: SparkContext, directory: String, startsWith: Array[String]): RDD[Document] = {
    val dirs = startsWith.map(starts => {
      directory + starts + "*"
    })
    loadDocuments(sc, dirs)
  }

  def loadDocuments(sc: SparkContext, directory: String): RDD[Document] = {

    val rddFile = loadFiles(sc, directory)
    rddFile.flatMap { case (filename, text) => {
      XMLParser.parseDocument(filename, text)
    }}



  }
  def loadObjectDocs(sc: SparkContext, directory: String): RDD[Document] = {
    sc.objectFile(directory)
  }

  def loadDocuments(sc: SparkContext, directories: Array[String]): RDD[Document] = {
    val rddFiles: RDD[(String, String)] = loadFiles(sc, directories)
    rddFiles.flatMap { case (filename, text) => {
      XMLParser.parseDocument(filename, text)
    }
    }
  }

  def loadBlogDocs(sc: SparkContext): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.blogXMLs)
  }


  def loadBlogDocs(sc: SparkContext, startsWith: String): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.blogXMLs, startsWith)
  }

  def loadBlogDocs(sc: SparkContext, startsWiths: Array[String]): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.blogXMLs, startsWiths)
  }

  def loadArticleDocs(sc: SparkContext): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.articleXMLs)
  }

  def loadArticleDocs(sc: SparkContext, startsWith: String): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.articleXMLs, startsWith)
  }


  def loadArticleDocs(sc: SparkContext, startsWiths: Array[String]): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.articleXMLs, startsWiths)
  }


  def loadTweetDocs(sc: SparkContext): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.twitterXMLs)
  }

  def loadPanDocs(sc: SparkContext): RDD[Document] = {
    loadDocuments(sc, Resources.DocumentResources.panXMLs)
  }

  def saveObjectDocs(directory: String, rdd: RDD[Document]) = {
    rdd.saveAsObjectFile(directory)
  }

  def saveHardDataset(rdd: RDD[Document]) = {

  }

}




