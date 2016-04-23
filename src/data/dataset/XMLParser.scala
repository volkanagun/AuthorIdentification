package data.dataset

import java.io.File
import java.util.Locale
import java.util.regex.Pattern

import data.document.Document
import options.{Parameters, Resources}
import options.Resources.DocumentModel
import org.xml.sax.SAXParseException
import util.TextFile

import scala.xml.Node

/**
  * Created by wolf on 10.04.2016.
  */

object GenreMap{
  val genreMap = Map[String,String](
    "sinema"->"cinema",
    "bilim"->"science",
    "kadın"->"woman",
    "html5"->"web-design",
    "flash"->"web-design",
    "jquery"->"web-design",
    "css"->"web-design",
    "grafik tasarım"->"web-design",
    "site tanıtım"->"web-design",
    "web-tasarım"->"web-design",
    "template"->"web-design",
    "php"->"web-design",
    "müzik"->"music",


    "oyun"->"pc-games",
    "oyunlar"->"pc-games",
    "multimedya"->"pc-games",
    "photoshop"->"pc-software",
    "program"->"pc-software",

    "internet"->"internet",
    "telekom"->"internet",
    "teknoloji"->"technology",
    "dijital"->"technology",

    "inceleme"->"tehcnology",
    "mobil"->"mobil-technology",
    "program"->"software-technology",
    "android"->"mobil-technology",

    "eğlencelik"->"fun",
    "funny"->"fun",
    "history"->"history",
    "culture"->"culture",
    "sanat"->"art",
    "art"->"art",
    "moda"->"trend",

    "deli hikayeleri"->"essay",
    "makaleler"->"essay",
    "cesur bölge"->"essay",

    "yatirim"->"economy",
    "e-ticaret"->"marketting",
    "pazarlama"->"marketting",
    "girisimler"->"marketting",

    "genel"->"general",
    "social"->"general"
  )

  def map(genre:String):String={
    genreMap.getOrElse(genre, genre)
  }
}

object XMLParser {

  val tr = Parameters.trLocale

  def mapXMLTags(text: String): String = {
    var ntext = text.replaceAll("&amp;", "&")
    ntext = text.replaceAll("&#8220;", "\"")
    ntext = text.replaceAll("&#8221;", "\"")
    ntext = text.replaceAll("&#8217;", "\'")

    ntext
  }

  def cleanText(text: String): String = {
    //rewrite here vice versa text replace
    var ntext = text.replaceAll("\u001B", "")



    ntext = ntext.replaceAll("&nbsp;", " ")
    ntext = ntext.replaceAll("&iexcl;", "¡")
    ntext = ntext.replaceAll("&cent;", "¢")
    ntext = ntext.replaceAll("&pound;", "£")
    ntext = ntext.replaceAll("&curren;", "¤")
    ntext = ntext.replaceAll("&yen;", "¥")
    ntext = ntext.replaceAll("&brvbar;", "¦")
    ntext = ntext.replaceAll("&sect;", "§")
    ntext = ntext.replaceAll("&uml;", "¨")
    ntext = ntext.replaceAll("&copy;", "©")
    ntext = ntext.replaceAll("&ordf;", "ª")
    ntext = ntext.replaceAll("&laquo;", "«")
    ntext = ntext.replaceAll("&not;", "¬")
    ntext = ntext.replaceAll("&shy;", "")
    ntext = ntext.replaceAll("&reg;", "®")
    ntext = ntext.replaceAll("&macr;", "¯")
    ntext = ntext.replaceAll("&deg;", "°")
    ntext = ntext.replaceAll("&plusmn;", "±")
    ntext = ntext.replaceAll("&sup2;", "²")
    ntext = ntext.replaceAll("&sup3;", "³")
    ntext = ntext.replaceAll("&acute;", "´")
    ntext = ntext.replaceAll("&micro;", "µ")
    ntext = ntext.replaceAll("&para;", "¶")
    ntext = ntext.replaceAll("&middot;", "·")
    ntext = ntext.replaceAll("&cedil;", "¸")
    ntext = ntext.replaceAll("&sup1;", "¹")
    ntext = ntext.replaceAll("&ordm;", "º")
    ntext = ntext.replaceAll("&raquo;", "»")
    ntext = ntext.replaceAll("&frac14;", "¼")
    ntext = ntext.replaceAll("&frac12;", "½")
    ntext = ntext.replaceAll("&frac34;", "¾")
    ntext = ntext.replaceAll("&iquest;", "¿")
    ntext = ntext.replaceAll("&Agrave;", "À")
    ntext = ntext.replaceAll("&Aacute;", "Á")
    ntext = ntext.replaceAll("&Acirc;", "Â")
    ntext = ntext.replaceAll("&Atilde;", "Ã")
    ntext = ntext.replaceAll("&Auml;", "Ä")
    ntext = ntext.replaceAll("&Aring;", "Å")
    ntext = ntext.replaceAll("&AElig;", "Æ")
    ntext = ntext.replaceAll("&Ccedil;", "Ç")
    ntext = ntext.replaceAll("&Egrave;", "È")
    ntext = ntext.replaceAll("&Eacute;", "É")
    ntext = ntext.replaceAll("&Ecirc;", "Ê")
    ntext = ntext.replaceAll("&Euml;", "Ë")
    ntext = ntext.replaceAll("&Igrave;", "Ì")
    ntext = ntext.replaceAll("&Iacute;", "Í")
    ntext = ntext.replaceAll("&Icirc;", "Î")
    ntext = ntext.replaceAll("&Iuml;", "Ï")
    ntext = ntext.replaceAll("&ETH;", "Ð")
    ntext = ntext.replaceAll("&Ntilde;", "Ñ")
    ntext = ntext.replaceAll("&Ograve;", "Ò")
    ntext = ntext.replaceAll("&Oacute\\;", "Ó")
    ntext = ntext.replaceAll("&Ocirc;", "Ô")
    ntext = ntext.replaceAll("&Otilde;", "Õ")
    ntext = ntext.replaceAll("&Ouml;", "Ö")
    ntext = ntext.replaceAll("&times;", "×")
    ntext = ntext.replaceAll("&Oslash;", "Ø")
    ntext = ntext.replaceAll("&Ugrave;", "Ù")
    ntext = ntext.replaceAll("&Uacute;", "Ú")
    ntext = ntext.replaceAll("&Ucirc;", "Û")
    ntext = ntext.replaceAll("&Uuml;", "Ü")
    ntext = ntext.replaceAll("&Yacute;", "Ý")
    ntext = ntext.replaceAll("&THORN;", "Þ")
    ntext = ntext.replaceAll("&szlig;", "ß")
    ntext = ntext.replaceAll("&agrave;", "à")
    ntext = ntext.replaceAll("&aacute;", "á")
    ntext = ntext.replaceAll("&acirc;", "â")
    ntext = ntext.replaceAll("&atilde;", "ã")
    ntext = ntext.replaceAll("&auml;", "ä")
    ntext = ntext.replaceAll("&aring;", "å")
    ntext = ntext.replaceAll("&aelig;", "æ")
    ntext = ntext.replaceAll("&ccedil;", "ç")
    ntext = ntext.replaceAll("&egrave;", "è")
    ntext = ntext.replaceAll("&eacute;", "é")
    ntext = ntext.replaceAll("&ecirc;", "ê")
    ntext = ntext.replaceAll("&euml;", "ë")
    ntext = ntext.replaceAll("&igrave;", "ì")
    ntext = ntext.replaceAll("&iacute;", "í")
    ntext = ntext.replaceAll("&icirc;", "î")
    ntext = ntext.replaceAll("&iuml;", "ï")
    ntext = ntext.replaceAll("&eth;", "ð")
    ntext = ntext.replaceAll("&ntilde;", "ñ")
    ntext = ntext.replaceAll("&ograve;", "ò")
    ntext = ntext.replaceAll("&oacute;", "ó")
    ntext = ntext.replaceAll("&ocirc;", "ô")
    ntext = ntext.replaceAll("&otilde;", "õ")
    ntext = ntext.replaceAll("&ouml;", "ö")
    ntext = ntext.replaceAll("&divide;", "÷")
    ntext = ntext.replaceAll("&oslash;", "ø")
    ntext = ntext.replaceAll("&ugrave;", "ù")
    ntext = ntext.replaceAll("&uacute;", "ú")
    ntext = ntext.replaceAll("&ucirc;", "û")
    ntext = ntext.replaceAll("&uuml;", "ü")
    ntext = ntext.replaceAll("&yacute;", "ý")
    ntext = ntext.replaceAll("&thorn;", "þ")
    ntext = ntext.replaceAll("&yuml;", "ÿ")

    ntext = ntext.replaceAll("&bull;", "•")
    ntext = ntext.replaceAll("&hellip;", "…")
    ntext = ntext.replaceAll("&prime;", "′")
    ntext = ntext.replaceAll("&Prime;", "″")
    ntext = ntext.replaceAll("&oline;", "‾")
    ntext = ntext.replaceAll("&frasl;", "⁄")
    ntext = ntext.replaceAll("&weierp;", "℘")
    ntext = ntext.replaceAll("&image;", "ℑ")
    ntext = ntext.replaceAll("&real;", "ℜ")
    ntext = ntext.replaceAll("&trade;", "™")
    ntext = ntext.replaceAll("&alefsym;", "ℵ")
    ntext = ntext.replaceAll("&larr;", "←")
    ntext = ntext.replaceAll("&uarr;", "↑")
    ntext = ntext.replaceAll("&rarr;", "→")
    ntext = ntext.replaceAll("&darr;", "↓")
    ntext = ntext.replaceAll("&barr;", "↔")
    ntext = ntext.replaceAll("&crarr;", "↵")
    ntext = ntext.replaceAll("&lArr;", "⇐")
    ntext = ntext.replaceAll("&uArr;", "⇑")
    ntext = ntext.replaceAll("&rArr;", "⇒")
    ntext = ntext.replaceAll("&dArr;", "⇓")
    ntext = ntext.replaceAll("&hArr;", "⇔")
    ntext = ntext.replaceAll("&forall;", "∀")
    ntext = ntext.replaceAll("&part;", "∂")
    ntext = ntext.replaceAll("&exist;", "∃")
    ntext = ntext.replaceAll("&empty;", "∅")
    ntext = ntext.replaceAll("&nabla;", "∇")
    ntext = ntext.replaceAll("&isin;", "∈")
    ntext = ntext.replaceAll("&notin;", "∉")
    ntext = ntext.replaceAll("&ni;", "∋")
    ntext = ntext.replaceAll("&prod;", "∏")
    ntext = ntext.replaceAll("&sum;", "∑")
    ntext = ntext.replaceAll("&minus;", "−")
    ntext = ntext.replaceAll("&lowast", "∗")
    ntext = ntext.replaceAll("&radic;", "√")
    ntext = ntext.replaceAll("&prop;", "∝")
    ntext = ntext.replaceAll("&infin;", "∞")
    ntext = ntext.replaceAll("&OEig;", "Œ")
    ntext = ntext.replaceAll("&oelig;", "œ")
    ntext = ntext.replaceAll("&Yuml;", "Ÿ")
    ntext = ntext.replaceAll("&spades;", "♠")
    ntext = ntext.replaceAll("&clubs;", "♣")
    ntext = ntext.replaceAll("&hearts;", "♥")
    ntext = ntext.replaceAll("&diams;", "♦")
    ntext = ntext.replaceAll("&thetasym;", "ϑ")
    ntext = ntext.replaceAll("&upsih;", "ϒ")
    ntext = ntext.replaceAll("&piv;", "ϖ")
    ntext = ntext.replaceAll("&Scaron;", "Š")
    ntext = ntext.replaceAll("&scaron;", "š")
    ntext = ntext.replaceAll("&ang;", "∠")
    ntext = ntext.replaceAll("&and;", "∧")
    ntext = ntext.replaceAll("&or;", "∨")
    ntext = ntext.replaceAll("&cap;", "∩")
    ntext = ntext.replaceAll("&cup;", "∪")
    ntext = ntext.replaceAll("&int;", "∫")
    ntext = ntext.replaceAll("&there4;", "∴")
    ntext = ntext.replaceAll("&sim;", "∼")
    ntext = ntext.replaceAll("&cong;", "≅")
    ntext = ntext.replaceAll("&asymp;", "≈")
    ntext = ntext.replaceAll("&ne;", "≠")
    ntext = ntext.replaceAll("&equiv;", "≡")
    ntext = ntext.replaceAll("&le;", "≤")
    ntext = ntext.replaceAll("&ge;", "≥")
    ntext = ntext.replaceAll("&sub;", "⊂")
    ntext = ntext.replaceAll("&sup;", "⊃")
    ntext = ntext.replaceAll("&nsub;", "⊄")
    ntext = ntext.replaceAll("&sube;", "⊆")
    ntext = ntext.replaceAll("&supe;", "⊇")
    ntext = ntext.replaceAll("&oplus;", "⊕")
    ntext = ntext.replaceAll("&otimes;", "⊗")
    ntext = ntext.replaceAll("&perp;", "⊥")
    ntext = ntext.replaceAll("&sdot;", "⋅")
    ntext = ntext.replaceAll("&lcell;", "⌈")
    ntext = ntext.replaceAll("&rcell;", "⌉")
    ntext = ntext.replaceAll("&lfloor;", "⌊")
    ntext = ntext.replaceAll("&rfloor;", "⌋")
    ntext = ntext.replaceAll("&lang;", "⟨")
    ntext = ntext.replaceAll("&rang;", "⟩")
    ntext = ntext.replaceAll("&loz;", "◊")
    ntext = ntext.replaceAll("&uml;", "¨")
    ntext = ntext.replaceAll("&lrm;", "")

    //Place is important
    ntext = ntext.replaceAll("&", "&amp;")




    ntext = Pattern.compile("(<a\\s(.*?)>)",Pattern.DOTALL).matcher(ntext).replaceAll("")
    ntext

  }

  def parseDocument(filename: String, text: String): Seq[Document] = {
    var mtext = cleanText(text)
    val xmlMain = scala.xml.XML.loadString(mtext);
    val xmlRoot = (xmlMain \\ "ROOT")

    xmlRoot.flatMap(root => {

      val docType = (root \ "@LABEL").text
      if (docType.equals(Resources.DocumentModel.ARTICLEDOC) || docType.equals("A")) {
        //Articles
        parseArticles(root, filename)

      }
      else if (docType.equals(Resources.DocumentModel.BLOGDOC) || docType.equals("B")) {
        //Blogs
        parseBlogs(root, filename)
      }
      else if (docType.equals(Resources.DocumentModel.PANDOC) || docType.equals("P")) {
        //PAN
        parsePAN(root, filename)

      }
      else {
        //Twitter
        parseTweets(root, filename)
      }
    })
  }

  def parseTweets(root: Node, filename: String): Seq[Document] = {
    var document: Document = null;
    var seq = Seq[Document]()
    var count = 0;
    (root \\ "RESULT").foreach(node => {
      val value = (node \ "@LABEL").text
      if (value.equals("TWEET")) {
        if (document != null) {
          seq = seq :+ document
        }
        val docid = filename.substring(0, filename.lastIndexOf(".")) + "-" + count
        document = new Document(docid, DocumentModel.TWITTERDOC)
        document.setGenre(DocumentModel.TWITTER)
        count = count + 1
      }
      else if (value.equals("AUTHORNAME")) {
        var authorName = node.text.trim
        if (authorName != null) {
          document.author = authorName.toLowerCase(tr)
        }
      }
      else if (value.equals("TWEETTEXT")) {
        val content = mapXMLTags(node.text)
        document.setText(content)
      }
    })

    if (document != null) {
      seq = seq :+ document
    }

    seq
  }

  def parsePAN(root: Node, filename: String): Seq[Document] = {
    var document: Document = null;
    var seq = Seq[Document]()

    (root \\ "RESULT").foreach(node => {
      val value = (node \ "@LABEL").text
      if (value.equals("ARTICLE")) {
        if (document != null) {
          seq = seq :+ document
        }
        document = new Document(null, DocumentModel.PANDOC)
      }
      else if (value.equals("ARTICLEID")) {
        var articleid = node.text
        if (articleid != null) {
          document.docid = articleid.trim
        }
      }
      else if (value.equals("AUTHORNAME")) {
        var authorName = node.text
        if (authorName != null) {
          document.author = authorName.trim
        }
      }

      else if (value.equals("ARTICLETEXT")) {
        val content = node.text
        document.setText(content)
      }
    })

    if (document != null) {
      seq = seq :+ document
    }

    seq
  }

  def parseArticles(root: Node, docid: String): Seq[Document] = {
    val document = new Document(docid, Resources.DocumentModel.ARTICLEDOC);
    var seq = Seq[Document](document)
    (root \\ "RESULT").foreach(node => {
      val value = (node \ "@LABEL").text
      if (value.equals("AUTHORNAME")) {
        var authorName = node.text.trim
        if (authorName != null && !authorName.isEmpty) {
          val index: Int = authorName.indexOf("|")

          if (index > 0) {
            authorName = authorName.substring(0, index).trim.replaceAll("\n", "")
          }

          document.author = authorName.toLowerCase(tr)
        }
      }
      else if (value.equals("ARTICLETITLE")) {
        val title: String = node.text.trim
        document.setTitle(title)
      }
      else if (value.equals("GENRE")) {
        var genre: String = node.text.trim
        if(genre!=null && !genre.isEmpty) {
          genre = genre.toLowerCase(tr)
          document.setGenre(GenreMap.map(genre))
        }
      }
      else if (value.equals("ARTICLETEXT")) {
        val content = node.text
        document.setText(content)
      }
      else if (value.equals("ARTICLEPARAGRAPH")) {
        val paragraph = node.text
        document.addParagraph(paragraph)
      }
    })

    seq
  }

  def parseBlogs(root: Node, docid: String): Seq[Document] = {
    val document = new Document(docid, Resources.DocumentModel.BLOGDOC);
    var seq = Seq[Document](document)
    (root \\ "RESULT").foreach(node => {
      val value = (node \ "@LABEL").text
      if (value.equals("AUTHORNAME")) {
        var authorName = node.text.trim
        if (authorName != null && !authorName.isEmpty) {
          document.author = authorName.toLowerCase(tr)
        }
      }
      else if (value.equals("ARTICLETITLE")) {
        val title: String = node.text.trim
        document.setTitle(title)
      }
      else if (value.equals("GENRE")) {
        var genre: String = node.text.trim
        if (genre != null & !genre.isEmpty) {
          genre =  genre.toLowerCase(tr)
          document.setGenre(GenreMap.map(genre))
        }
      }
      else if (value.equals("ARTICLETEXT")) {
        val content = node.text
        document.setText(content)
      }
      else if (value.equals("ARTICLEPARAGRAPH")) {
        val paragraph = node.text
        document.addParagraph(paragraph)
      }
    })

    seq
  }

  def checkFolder(folder: String): Unit = {
    val dir = new File(folder)
    var seq = Seq[(String, String, Int, Int)]()
    var del = Seq[String]()

    dir.listFiles().foreach(file => {
      val filename = file.getName
      val filepath = file.getAbsolutePath

      val text = TextFile.readText(file)
      try {
        XMLParser.parseDocument(filename, text)
      }
      catch {
        case sax: SAXParseException => {
          seq = seq :+(filename, sax.getMessage, sax.getLineNumber, sax.getColumnNumber)
          del = del :+ filepath
        }
      }
    })

    seq.foreach(tuple => {

      println("Filename: " + tuple._1 + " Line:" + tuple._3, " Column:" + tuple._4 + "Message: " + tuple._2)

    })
  }

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Delete Conditions">

  protected def genreDelete(genreText: String): Boolean = {
    if (genreText != null) (genreText.trim.length > 200 || genreText.contains("/\">") ||
      genreText.contains("\">"))
    else true
  }

  protected def contentDelete(contentText: String): Boolean = {
    if (contentText != null) {
      (contentText.trim.length < 50 || contentText.contains("/\">") || contentText.contains("\">") ||
        contentText.contains("window.adsbygoogle"))
    }
    else {
      true
    }
  }

  protected def authorDelete(authorText: String): Boolean = {
    if (authorText != null) {
      (authorText.trim.contains("konuk")|| authorText.contains("/\">") || authorText.contains("\">") ||
        authorText.contains("window.adsbygoogle"))
    }
    else {
      true
    }
  }
  //</editor-fold>
  ///////////////////////////////////////////////////////////




  def fixFolder(folder: String): Unit = {
    val dir = new File(folder)
    var seq = Seq[(String, String, Int, Int)]()
    var del = Seq[String]()

    dir.listFiles().foreach(file => {
      val filename = file.getName
      val filepath = file.getAbsolutePath

      val text = TextFile.readText(file)
      try {
        val docSeq = XMLParser.parseDocument(filename, text)
        if (docSeq.isEmpty) {
          del = del :+ filepath
        }
        else {
          val doc = docSeq.head
          val docText = doc.getText()
          if (contentDelete(docText)) {
            del = del :+ filepath
          }

          val genreText = doc.getGenre()
          if (genreDelete(genreText)) {
            del = del :+ filepath
          }

          val authorText = doc.getAuthor()
          if(authorDelete(authorText)){
            del = del :+ filepath
          }


        }
      }
      catch {
        case sax: SAXParseException => {
          seq = seq :+(filename, sax.getMessage, sax.getLineNumber, sax.getColumnNumber)
          del = del :+ filepath
        }
      }
    })

    seq.foreach(tuple => {

      println("Filename: " + tuple._1 + " Line:" + tuple._3, " Column:" + tuple._4 + "Message: " + tuple._2)

    })

    println("Deleting " + del.length + " files from folder")
    del.foreach(filename => {
      println("Deleting filename : " + filename)
      val file = new File(filename)
      if (file.exists()) {
        file.delete();
      }

    })


  }


  def main(args: Array[String]) {

    //XMLParser.fixFolder(Resources.DocumentResources.blogDir)
    //XMLParser.checkFolder(Resources.DocumentResources.blogDir)

  }
}

