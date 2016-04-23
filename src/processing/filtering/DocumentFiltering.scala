package processing.filtering

import org.apache.spark.rdd.RDD
import processing.structures.docs.{Blog, Article, Document}

/**
  * Created by wolf on 08.01.2016.
  */
object FilterParams extends Serializable{
  //Filter based on a template
  val titleLengthMin = 50
  val contentTweetLengthMin = 50
  val contentTextLengthMin = 400
  val contentTextMaxLength = 2000
  val tweetSizeMin = 30
  val articleSizeMin = 10
  val genreSizeMin = 20
  val sizeMax = 50000
}




case class SingleFiltering(val docFilters: Seq[Filter]) extends Serializable{
  //Filter target
  //Filter type
  //Filter method
  //Iteratively filter
  def filter(document:Document): Boolean ={
    docFilters.forall(filter=>filter.filter(document))
  }

}

case class MultipleFilter(val docFiler:Seq[SequenceFilter]) extends Serializable
{
  //Iteratively filter
  def filter(documents:Seq[Document]):Boolean={
     docFiler.forall(filter=>{filter.filter(documents)})
  }
}

//Number of documents condition
//Number of target documents condition
//Content length condition

trait Filter extends Serializable {
  def filter(document: Document):Boolean
}

trait TextFilter extends Filter {
  def filter(document: Document): Boolean
  def target(document: Document): String
}

trait SizeFilter extends Filter {
  def target(document: Document): Int
  def filter(document: Document): Boolean
}

trait SequenceFilter extends Filter {
  def filter(document:Document) : Boolean = true
  def filter(document: Seq[Document]): Boolean
}


class TitleLengthFilter(val lower: Double, val upper: Double) extends TextFilter {

  override def target(document: Document): String = {
    if (document.isArticle) document.asInstanceOf[Article].getTitle
    else if (document.isBlog) document.asInstanceOf[Blog].getTitle
    else null
  }
  override def filter(document: Document): Boolean = {
    val text = target(document)

    (text!=null && text.length > lower && text.length < upper)
  }
}

class ContentLengthFilter(val lower: Double, val upper: Double) extends TextFilter {
  override def target(document: Document): String = {
    document.getText
  }

  override def filter(document: Document): Boolean = {
    val text = target(document)
    (text.length > lower && text.length < upper)
  }
}

class ParagraphSizeFilter(val lower: Double, val upper: Double) extends SizeFilter {
  override def target(document: Document): Int = {
    if (document.isArticle) document.asInstanceOf[Article].getParagraphList.size()
    else if (document.isBlog) document.asInstanceOf[Blog].getParagraphList.size()
    else 0
  }

  override def filter(document: Document): Boolean = {
    val size = target(document)
    (size > lower && size < upper)
  }
}


class DocumentSize(val docType: String, val lower: Long, val upper: Long) extends SequenceFilter {
  override def filter(documents: Seq[Document]): Boolean = {
    val docs = documents.filter(doc => {
      if (docType.equals(Document.ARTICLE)) doc.isArticle
      else if (docType.equals(Document.BLOG)) doc.isBlog
      else if (docType.equals(Document.TWEET)) doc.isTweet
      else if (docType.equals(Document.REUTORS)) doc.isReuters
      else if (docType.equals(Document.PAN)) doc.isPAN
      else false
    })
    (docs.length > lower && docs.length < upper)
  }
}

class GenreSize(val genreType:String, val lower:Long, val upper:Long) extends SequenceFilter{
  override def filter(documents: Seq[Document]): Boolean = {
    val docs = documents.filter(doc=>{
      if(doc.isArticle) doc.asInstanceOf[Article].getGenre.equals(genreType)
      else if(doc.isBlog) doc.asInstanceOf[Blog].getGenre.equals(genreType)
      else true
    })

    (docs.length > lower && docs.length < upper)
  }
}

class DocumentTypeBySize(val types: Seq[DocumentSize]) extends SequenceFilter {
  override def filter(documents: Seq[Document]): Boolean = {
    types.forall(docSize => {
      docSize.filter(documents)
    })
  }
}




