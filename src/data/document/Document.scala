package data.document

import data.dataset.XMLParser
import language.boundary.SentenceML
import language.morphology.MorphTagger
import language.postagger.PosTaggerImp
import language.tokenization.TokenizerImp
import options.Resources
import options.Resources.DocumentModel

import scala.collection.JavaConversions

/**
  * Created by wolf on 01.04.2016.
  */
case class Document(var docid: String, var doctype: String, var author: String) extends Serializable {
  var genre: String = null
  var title: String = null
  var text: String = null

  def this(docid: String, doctype: String) = this(docid, doctype, null)

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
    DocumentModel.isBlog(doctype)
  }

  def isTweet(): Boolean = {
    DocumentModel.isTweet(doctype)
  }

  def isArticle(): Boolean = {
    DocumentModel.isArticle(doctype)
  }

  def isPAN(): Boolean = {
    DocumentModel.isPAN(doctype)
  }

  def hasLengthGreater(length: Int): Boolean = {
    text.length > length
  }

  def toParagraphs(): Seq[Paragraph] = {
    if (!paragraphs.isEmpty) {
      paragraphs.zipWithIndex.map { case (text, seq) => {
        new Paragraph(docid, author, seq, text)
      }
      }
    }
    else {
      //Paragraphize here
      Seq(new Paragraph(docid, author, 0, text))
    }
  }


  //</editor-fold>
  ///////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Feature Extraction">
  def toSentences(sentenceML: SentenceML): Seq[Sentence] = {
    toParagraphs().flatMap(paragraph => {
      paragraph.toSentences(sentenceML)
    })
  }

  def toTokens(sentenceML: SentenceML, tokenizer: TokenizerImp): Seq[String] = {
    toSentences(sentenceML).flatMap(sentence => sentence.tokens(tokenizer))
  }

  def toMorphs(sentenceML: SentenceML, tokenizer: TokenizerImp, disambiguator: MorphTagger): Seq[String] = {
    toSentences(sentenceML).flatMap(sentence => {
      val tokens = sentence.tokens(tokenizer)
      disambiguator.predict(tokens)
    })
  }

  def toPosses(sentenceML: SentenceML, tokenizerImp: TokenizerImp, posTagger: PosTaggerImp): Seq[String] = {
    toSentences(sentenceML).flatMap(sentence => {
      val tokens = sentence.tokens(tokenizerImp)
      posTagger.test(tokens)
    })
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
        XMLParser.docToXML(text) +
        closeXML +
        closeXML
    }
    else if (isArticle()) {
      val rootXML = "<ROOT LABEL=\"" + Resources.DocumentModel.ARTICLEDOC + "\">\n"
      val rootCloseXML = "</ROOT>\n"
      val closeXML = "</RESULT>\n"
      val containerXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"CONTAINER\">"

      val authorXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"AUTHORNAME\">\n" + author + "\n" + closeXML
      val titleXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETITLE\">\n" + title + "\n" + closeXML
      val genreXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"GENRE\">\n" + genre + "\n" + closeXML
      var textXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETEXT\">\n"
      for (paragraph <- paragraphs) {
        val paragraphXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\n" +
          paragraph + "\n" + closeXML

        textXML += paragraphXML

      }

      textXML += closeXML

      rootXML + containerXML +
        authorXML +
        titleXML +
        genreXML +
        textXML +
        closeXML +
        rootCloseXML

    }
    else if (isBlog()) {
      val rootXML = "<ROOT LABEL=\"" + DocumentModel.BLOGDOC + "\">\n"
      val rootCloseXML = "</ROOT>\n"
      val closeXML = "</RESULT>\n"
      val containerXML = "<RESULT TYPE=\"CONTAINER\" LABEL=\"ARTICLE\">"

      val authorXML = "<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\n" + author + "\n" + closeXML
      val titleXML = "<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLETITLE\">\n" + title + "\n" + closeXML
      val genreXML = "<RESULT TYPE=\"TEXT\" LABEL=\"GENRE\">\n" + genre + "\n" + closeXML
      var textXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETEXT\">\n"
      for (paragraph <- paragraphs) {
        val paragraphXML = "<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\n" +
          paragraph + "\n" + closeXML

        textXML += paragraphXML

      }

      textXML += closeXML

      rootXML + containerXML +
        authorXML +
        titleXML +
        genreXML +
        textXML +
        closeXML +
        rootCloseXML

    }
    else {
      ""
    }
  }


}

