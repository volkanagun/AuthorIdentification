package data.document

import language.boundary.SentenceML
import language.morphology.{MorphLight, MorphResult, AnalyzerImp}
import language.tokenization.TokenizerImp
import options.Resources.DocumentModel

import scala.collection.JavaConversions

/**
  * Created by wolf on 01.04.2016.
  */
class Document(var docid: String, var doctype: String, var author: String) extends Serializable {
  var genre: String = null
  var title: String = null
  var text: String = null

  def this(docid:String, doctype:String) = this(docid,doctype,null)

  var paragraphs = Seq[String]()

  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Setters and Getters">
  def setGenre(genre: String): this.type = {
    this.genre = genre
    this
  }

  def setTitle(title: String): this.type = {
    this.title = title
    this
  }

  def setText(text: String): this.type = {
    this.text = text
    this
  }

  def setParagraphs(paragraphs: Seq[String]): this.type = {
    this.paragraphs = paragraphs
    this
  }

  def setParagraphs(paragraphs: java.util.List[String]): this.type = {
    this.paragraphs = JavaConversions.asScalaBuffer(paragraphs).toSeq
    this
  }

  def getGenre(): String = {
    genre
  }

  def getTitle(): String = {
    title
  }

  def getText(): String = {
    text
  }

  def getParagraphs(): Seq[String] = {
    paragraphs
  }


  def getAuthor(): String = {
    author
  }

  def getDocId(): String = {
    docid
  }

  def getDocType(): String = {
    doctype
  }


  //</editor-fold>
  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="is methods">
  def isBlog(): Boolean = {
    DocumentModel.BLOGDOC.equals(doctype)
  }

  def isTweet(): Boolean = {
    DocumentModel.TWITTERDOC.equals(doctype)
  }

  def isArticle(): Boolean = {
    DocumentModel.ARTICLEDOC.equals(doctype)
  }

  def isPAN(): Boolean = {
    DocumentModel.PANDOC.equals(doctype)
  }

  def hasLengthGreater(length: Int): Boolean = {
    text.length > length
  }

  def toParagraphs() : Seq[Paragraph] = {
    paragraphs.zipWithIndex.map{case(text,seq)=>{
      new Paragraph(docid, seq, text)
    }}
  }



  //</editor-fold>
  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Feature Extraction">
  def toSentences(sentenceML: SentenceML) : Seq[Sentence] = {
    toParagraphs().flatMap(paragraph=>{paragraph.toSentences(sentenceML)})
  }

  def toTokens(sentenceML: SentenceML, tokenizer: TokenizerImp) : Seq[String]={
    toSentences(sentenceML).flatMap(sentence=>sentence.tokens(tokenizer))
  }

  def toMorphs(sentenceML: SentenceML, tokenizer: TokenizerImp, stemmer: AnalyzerImp) : Seq[MorphLight]= {
    toSentences(sentenceML).flatMap(sentence=>sentence.morphs(tokenizer, stemmer))
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  def addParagraph(paragraph: String): this.type = {
    paragraphs = paragraphs :+ paragraph
    this
  }


  override def toString = s"Document($docid, $doctype, $author)"

  def toXML(): String = {
    if (isTweet()) {
      val closeXML = "</RESULT>\n"
      val containerXML = "<RESULT TYPE=\"CONTAINER\" LABEL=\"TWEET\">\n"
      val authorXML = "<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\n"
      val textXML = "<RESULT TYPE=\"TEXT\" LABEL=\"TWEETTEXT\">\n"
      containerXML +
        authorXML +
        author +
        closeXML +
        textXML +
        text +
        closeXML +
        closeXML
    }
    else if (isArticle()) {
      val rootXML = "<ROOT>\n"
      val rootCloseXML = "</ROOT>\n"
      val closeXML = "</RESULT>\n"

      ""
    }
    else {
      ""
    }
  }
}
