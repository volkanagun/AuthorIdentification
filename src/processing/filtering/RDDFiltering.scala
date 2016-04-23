package processing.filtering

import org.apache.spark.rdd.RDD
import processing.structures.docs.Document

/**
  * Created by wolf on 08.01.2016.
  */
object FilteringCaseI {

  val filterByTypeSize = new FilterByTypeSize
  val filterByContentLength = new FilterByContentLength

  def filterDocs(docRDD:RDD[Document]) : RDD[Document]={
    val filteredBySize = filterByTypeSize.filterDocs(docRDD)
    val filteredByLength = filterByContentLength.filterDocs(filteredBySize)

    filteredByLength
  }

  def filterDocPairs(pairRDD:RDD[(String, Iterable[Document])]) : RDD[(String, Iterable[Document])]={
    val filteredBySize = filterByTypeSize.filterPairs(pairRDD)
    val filteredByLength = filterByContentLength.filterPairs(filteredBySize)

    filteredByLength
  }
}

class FilterByTypeSize()
{
  val articleSizeFilter = new DocumentSize(Document.ARTICLE, FilterParams.articleSizeMin,FilterParams.sizeMax)
  val blogSizeFilter = new DocumentSize(Document.BLOG, FilterParams.articleSizeMin,FilterParams.sizeMax)
  val tweetSizeFilter = new DocumentSize(Document.TWEET, FilterParams.tweetSizeMin, FilterParams.sizeMax)
  val sizeFilter = Seq(articleSizeFilter,blogSizeFilter,tweetSizeFilter)
  val multiSizeFilter = new MultipleFilter(sizeFilter)

  def filterPairs(docRDD:RDD[(String,Iterable[Document])]):RDD[(String,Iterable[Document])]={
    docRDD.filter( pair =>{
      val documents = pair._2
      multiSizeFilter.filter(documents.toSeq)
    })
  }

  def filterDocs(docRDD:RDD[Document]) : RDD[Document] = {
    val pairs = docRDD.map(doc=>(doc.getAuthor, doc)).groupByKey()
    val filtered = filterPairs(pairs)
    filtered.flatMap(pair=>(pair._2))
  }
}

class FilterByContentLength(){
  val articleTextFilter = new ContentLengthFilter(FilterParams.contentTextLengthMin, FilterParams.contentTextMaxLength)
  val tweetTextFilter = new ContentLengthFilter(FilterParams.contentTweetLengthMin,FilterParams.contentTextMaxLength)
  val singleContentFilter = new SingleFiltering(Seq(articleTextFilter,tweetTextFilter))

  def filterPairs(docRDD:RDD[(String,Iterable[Document])]):RDD[(String,Iterable[Document])]={
    val rdd:RDD[Document] = docRDD.flatMap(pair=>{pair._2})
    val filtered:RDD[Document] = rdd.filter(doc=>{
      singleContentFilter.filter(doc)
    })

    val grouped:RDD[(String,Iterable[Document])] = filtered.map(doc=>(doc.getAuthor,doc)).groupByKey()
    grouped

  }

  def filterDocs(docRDD:RDD[Document]):RDD[Document]={
    docRDD.filter(doc=>{
      singleContentFilter.filter(doc)
    })
  }
}

