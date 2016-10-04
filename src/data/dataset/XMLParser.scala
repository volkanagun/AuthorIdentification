package data.dataset

import java.io.{File, PrintWriter}
import java.util.Locale
import java.util.regex.Pattern

import com.cybozu.labs.langdetect.{Detector, DetectorFactory}
import data.document.Document
import options.{Parameters, Resources}
import options.Resources.{DocumentModel, DocumentResources}
import org.xml.sax.SAXParseException
import util.TextFile

import scala.xml.Node

/**
  * Created by wolf on 10.04.2016.
  */

class Replacement(val pattern: String, val replace: String) {
  def replaceAll(text: String): String = {
    text.replaceAll(pattern, replace)
  }

  def swapReplaceAll(text: String): String = {
    text.replaceAll(replace, pattern)
  }
}

object LangDetect {
  DetectorFactory.loadProfile(Resources.LanguageDetector.profiles);

  def detectLanguage(text: String): String = {
    val detector = DetectorFactory.create();
    detector.append(text);
    try {
      detector.detect()
    }catch{
      case e:Exception=> {
        "en"
      }
    }
  }

  def isEnglish(text: String): Boolean = {
    val label = detectLanguage(text)
    label.equals("en")
  }

  def isForeign(text:String):Boolean = {
    val label = detectLanguage(text)
    !label.equals("tr")
  }
}

object GenreMap {
  val genreMap = Map[String, String](
    "sinema" -> "cinema",
    "bilim" -> "science",
    "tıp" -> "science",
    "çocuk psikolojisi" -> "science",
    "psikoloji" -> "science",
    "çevre bilinci" -> "science",

    "kadın" -> "woman",
    "bebek - çocuk" -> "woman",
    "aşk - evlilik" -> "woman",
    "ilişkiler" -> "woman",

    "html5" -> "web-design",
    "flash" -> "web-design",
    "jquery" -> "web-design",
    "css" -> "web-design",
    "grafik tasarım" -> "web-design",
    "web tasarım" -> "web-design",
    "site tanıtım" -> "web-design",
    "web-tasarım" -> "web-design",
    "template" -> "web-design",
    "php" -> "web-design",
    "ilginç tasarımlar" -> "design",
    "tasarım" -> "design",
    "müzik" -> "music",

    "spor" -> "sports",
    "futbol" -> "sports",

    "genel sağlık" -> "health",
    "yoga / meditasyon" -> "health",
    "estetik / güzellik" -> "health",
    "sağlık" -> "health",
    "sağlıklı yaşam" -> "health",
    "beslenme / diyet" -> "health",

    "gezi - tatil" -> "travel",
    "coğrafya" -> "travel",
    "dünya şehirleri" -> "travel",


    "oyun" -> "pc-games",
    "oyunlar" -> "pc-games",
    "multimedya" -> "pc-games",
    "photoshop" -> "pc-software",
    "program" -> "pc-software",

    "internet" -> "internet",
    "telekom" -> "internet",
    "teknoloji" -> "technology",
    "dijital" -> "technology",
    "webrazzitv" -> "technology",
    "kariyer" -> "career",
    "iş yaşamı - kariyer" -> "career",


    "inceleme" -> "technology",
    "bilişim" -> "technology",
    "mobil" -> "mobil-technology",
    "program" -> "software-technology",
    "android" -> "mobil-technology",

    "eğlencelik" -> "fun",
    "mizah" -> "fun",
    "funny" -> "fun",
    "history" -> "history",
    "tarih" -> "history",


    "culture" -> "culture",
    "kültür" -> "culture",
    "gündelik yaşam" -> "culture",
    "astroloji" -> "culture",
    "kültür - sanat" -> "culture",
    "mimarlık" -> "culture",
    "sosyoloji" -> "culture",
    "yemek - mutfak" -> "culture",
    "sanat" -> "art",
    "art" -> "art",
    "resim" -> "art",
    "şiir" -> "art",

    "inançlar" -> "religion",
    "eğitim" -> "education",
    "kişisel gelişim" -> "education",
    "okullar" -> "education",
    "sınavlar" -> "education",


    "moda" -> "trend",
    "magazin"->"trend",
    "alışveriş - moda" -> "trend",

    "etkinlikler" -> "events",
    "etkinlikler / festivaller" -> "events",

    "hukuk" -> "law",

    "deli hikayeleri" -> "essay",
    "makaleler" -> "essay",
    "cesur bölge" -> "essay",
    "deneme" -> "essay",
    "öykü" -> "essay",
    "edebiyat" -> "essay",
    "kitap" -> "essay",
    "felsefe" -> "essay",

    "siyaset" -> "politics",

    "yatirim" -> "economy",
    "odeme-sistemleri" -> "economy",
    "ödeme-sistemleri" -> "economy",
    "türkiye ekonomisi" -> "economy",
    "e-ticaret" -> "economy",
    "ekonomi - finans" -> "economy",
    "pazarlama" -> "marketting",
    "girisimler" -> "marketting",

    "genel" -> "general",
    "social" -> "general",
    "güncel" -> "general"
  )

  def map(genre: String): String = {
    genreMap.getOrElse(genre, genre)
  }
}

object HtmlReplaces {
  val htmlReplaces = Array(
    new Replacement("&nbsp;", " "), new Replacement("&iexcl;", "¡"), new Replacement("&cent;", ""), new Replacement("&pound;", "£"),
    new Replacement("&curren;", "¤"), new Replacement("&yen;", "¥"), new Replacement("&brvbar;", "¦"), new Replacement("&sect;", "§"),
    new Replacement("&uml;", "¨"), new Replacement("&copy;", "©"), new Replacement("&ordf;", "ª"), new Replacement("&laquo;", "«"),
    new Replacement("&not;", "¬"), new Replacement("&shy;", ""), new Replacement("&reg;", "®"), new Replacement("&macr;", "¯"),
    new Replacement("&deg;", "°"), new Replacement("&plusmn;", "±"), new Replacement("&sup2;", "²"), new Replacement("&sup3;", "³"),
    new Replacement("&acute;", "´"), new Replacement("&micro;", "µ"), new Replacement("&para;", "¶"), new Replacement("&middot;", "·"),
    new Replacement("&cedil;", "¸"), new Replacement("&sup1;", "¹"), new Replacement("&ordm;", "º"), new Replacement("&raquo;", "»"),
    new Replacement("&frac14;", "¼"), new Replacement("&frac12;", "½"), new Replacement("&frac34;", "¾"), new Replacement("&iquest;", "¿"),
    new Replacement("&Agrave;", "À"), new Replacement("&Aacute;", "Á"), new Replacement("&Acirc;", "Â"), new Replacement("&Atilde;", "Ã"),
    new Replacement("&Auml;", "Ä"), new Replacement("&Aring;", "Å"), new Replacement("&AElig;", "Æ"), new Replacement("&Ccedil;", "Ç"),
    new Replacement("&Egrave;", "È"), new Replacement("&Eacute;", "É"), new Replacement("&Ecirc;", "Ê"), new Replacement("&Euml;", "Ë"),
    new Replacement("&Igrave;", "Ì"), new Replacement("&Iacute;", "Í"), new Replacement("&Icirc;", "Î"), new Replacement("&Iuml;", "Ï"),
    new Replacement("&ETH;", "Ð"), new Replacement("&Ntilde;", "Ñ"), new Replacement("&Ograve;", "Ò"), new Replacement("&Oacute\\;", "Ó"),
    new Replacement("&Ocirc;", "Ô"), new Replacement("&Otilde;", "Õ"), new Replacement("&Ouml;", "Ö"), new Replacement("&times;", "×"),
    new Replacement("&Oslash;", "Ø"), new Replacement("&Ugrave;", "Ù"), new Replacement("&Uacute;", "Ú"), new Replacement("&Ucirc;", "Û"),
    new Replacement("&Uuml;", "Ü"), new Replacement("&Yacute;", "Ý"), new Replacement("&THORN;", "Þ"), new Replacement("&szlig;", "ß"),
    new Replacement("&agrave;", "à"), new Replacement("&aacute;", "á"), new Replacement("&acirc;", "â"), new Replacement("&atilde;", "ã"),
    new Replacement("&auml;", "ä"), new Replacement("&aring;", "å"), new Replacement("&aelig;", "æ"), new Replacement("&ccedil;", "ç"),
    new Replacement("&egrave;", "è"), new Replacement("&eacute;", "é"), new Replacement("&ecirc;", "ê"), new Replacement("&euml;", "ë"),
    new Replacement("&igrave;", "ì"), new Replacement("&iacute;", "í"), new Replacement("&icirc;", "î"), new Replacement("&iuml;", "ï"),
    new Replacement("&eth;", "ð"), new Replacement("&ntilde;", "ñ"), new Replacement("&ograve;", "ò"), new Replacement("&oacute;", "ó"),
    new Replacement("&ocirc;", "ô"), new Replacement("&otilde;", "õ"), new Replacement("&ouml;", "ö"), new Replacement("&divide;", "÷"),
    new Replacement("&oslash;", "ø"), new Replacement("&ugrave;", "ù"), new Replacement("&uacute;", "ú"), new Replacement("&ucirc;", "û"),
    new Replacement("&uuml;", "ü"), new Replacement("&yacute;", "ý"), new Replacement("&thorn;", "þ"), new Replacement("&yuml;", "ÿ"),
    new Replacement("&bull;", "•"), new Replacement("&hellip;", "…"), new Replacement("&prime;", "′"), new Replacement("&Prime;", "″"),
    new Replacement("&oline;", "‾"), new Replacement("&frasl;", "⁄"), new Replacement("&weierp;", "℘"), new Replacement("&image;", "ℑ"),
    new Replacement("&real;", "ℜ"), new Replacement("&trade;", "™"), new Replacement("&alefsym;", "ℵ"), new Replacement("&larr;", "←"),
    new Replacement("&uarr;", "↑"), new Replacement("&rarr;", "→"), new Replacement("&darr;", "↓"), new Replacement("&barr;", "↔"),
    new Replacement("&crarr;", "↵"), new Replacement("&lArr;", "⇐"), new Replacement("&uArr;", "⇑"), new Replacement("&rArr;", "⇒"),
    new Replacement("&dArr;", "⇓"), new Replacement("&hArr;", "⇔"), new Replacement("&forall;", "∀"), new Replacement("&part;", "∂"),
    new Replacement("&exist;", "∃"), new Replacement("&empty;", "∅"), new Replacement("&nabla;", "∇"), new Replacement("&isin;", "∈"),
    new Replacement("&notin;", "∉"), new Replacement("&ni;", "∋"), new Replacement("&prod;", "∏"), new Replacement("&sum;", "∑"),
    new Replacement("&minus;", "−"), new Replacement("&lowast", "∗"), new Replacement("&radic;", "√"), new Replacement("&prop;", "∝"),
    new Replacement("&infin;", "∞"), new Replacement("&OEig;", "Œ"), new Replacement("&oelig;", "œ"), new Replacement("&Yuml;", "Ÿ"),
    new Replacement("&spades;", "♠"), new Replacement("&clubs;", "♣"), new Replacement("&hearts;", "♥"), new Replacement("&diams;", "♦"),
    new Replacement("&thetasym;", "ϑ"), new Replacement("&upsih;", "ϒ"), new Replacement("&piv;", "ϖ"), new Replacement("&Scaron;", "Š"),
    new Replacement("&scaron;", "š"), new Replacement("&ang;", "∠"), new Replacement("&and;", "∧"), new Replacement("&or;", "∨"),
    new Replacement("&cap;", "∩"), new Replacement("&cup;", "∪"), new Replacement("&int;", "∫"), new Replacement("&there4;", "∴"),
    new Replacement("&sim;", "∼"), new Replacement("&cong;", "≅"), new Replacement("&asymp;", "≈"), new Replacement("&ne;", "≠"),
    new Replacement("&equiv;", "≡"), new Replacement("&le;", "≤"), new Replacement("&ge;", "≥"), new Replacement("&sub;", "⊂"),
    new Replacement("&sup;", "⊃"), new Replacement("&nsub;", "⊄"), new Replacement("&sube;", "⊆"), new Replacement("&supe;", "⊇"),
    new Replacement("&oplus;", "⊕"), new Replacement("&otimes;", "⊗"), new Replacement("&perp;", "⊥"), new Replacement("&sdot;", "⋅"),
    new Replacement("&lcell;", "⌈"), new Replacement("&rcell;", "⌉"), new Replacement("&lfloor;", "⌊"), new Replacement("&rfloor;", "⌋"),
    new Replacement("&lang;", "⟨"), new Replacement("&rang;", "⟩"), new Replacement("&loz;", "◊"), new Replacement("&uml;", "¨"),
    new Replacement("&lrm;", "")
  )
}

object XMLParser {

  val tr = Parameters.trLocale

  def docToXML(text: String): String = {
    var ntext = text.replaceAll("&", "&amp;")
    ntext
  }

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
    ntext = HtmlReplaces.htmlReplaces
      .foldLeft[String](ntext)((txt, replacement)=> replacement.replaceAll(txt))

    /*
    Invalid XML Tags are created never replace to &
    ntext = ntext.replaceAll("&gt;", ">")
    ntext = ntext.replaceAll("&lt;", "<")*/

    //Place is important
    ntext = ntext.replaceAll("&", "&amp;")




    ntext = Pattern.compile("(<a\\s(.*?)>)", Pattern.DOTALL).matcher(ntext).replaceAll("")
    ntext

  }

  def parseEvaluations(filename: String): Map[String, String] = {
    val xmlMain = scala.xml.XML.loadFile(filename);
    val xmlParams = (xmlMain \\ "MEASURE")
    var paramMap = Map[String, String]()
    xmlParams.map(param => {
      val tag = (param.attribute("ATTR").get.head).text.trim
      val value = param.text.trim
      paramMap = paramMap + (tag -> value)
    })
    paramMap
  }

  def parseLabelEvaluations(filename: String): Map[String, Map[String, String]] = {
    Map[String, Map[String, String]]()
  }


  def parseParameters(filename: String): Map[String, String] = {
    //println("Parsing parameters of " + filename)
    val xmlMain = scala.xml.XML.loadFile(filename);
    val xmlParams = (xmlMain \\ "param")
    var paramMap = Map[String, String]()
    xmlParams.map(param => {
      val tag = (param \ "tag").text.trim
      val value = (param \ "value").text.trim
      paramMap = paramMap + (tag -> value)
    })
    paramMap
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
      else if (docType.equals(Resources.DocumentModel.TWEET) || docType.equals(Resources.DocumentModel.TWITTER) || docType.equals(Resources.DocumentModel.TWITTERDOC) || docType.equals("T")) {
        //PAN
        parseTweets(root, filename)

      }
      else {
        Seq[Document]()
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
        if (genre != null && !genre.isEmpty) {
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
          seq = seq :+ (filename, sax.getMessage, sax.getLineNumber, sax.getColumnNumber)
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

  protected def tweetDelete(contentText: String): Boolean = {
    if (contentText != null) {
      contentText.trim.startsWith("RT") || LangDetect.isForeign(contentText)
    }
    else {
      true
    }
  }

  protected def authorDelete(authorText: String): Boolean = {
    if (authorText != null) {
      (authorText.trim.contains("konuk") || authorText.contains("/\">") || authorText.contains("\">") ||
        authorText.contains("window.adsbygoogle"))
    }
    else {
      true
    }
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  def filterTweets(folder: String): Seq[Document] = {
    //Read tweets and write tweets again
    val dir = new File(folder)
    var seq = Seq[Document]()

    dir.listFiles().foreach(file => {
      val filename = file.getName
      val filepath = file.getAbsolutePath
      val text = TextFile.readText(file)
      try {

        val docSeq = XMLParser.parseDocument(filename, text)
        seq = seq ++ (if (docSeq.isEmpty) {
          Seq[Document]()
        }
        else {
          val myseq = docSeq.filter(document => {
            document.getDocType().equals(DocumentModel.TWITTERDOC) && !tweetDelete(document.getText())
          })

          myseq
        })
      }
      catch{
        case ex:Exception=>println("Error in filename: "+filename+" Message: "+ex.getMessage)
      }
    })

    seq
  }

  def fixTweets(tweetFolder: String, fixName: String): Unit = {
    val tweetDocs = filterTweets(DocumentResources.twitterDir)
    new PrintWriter(tweetFolder + fixName) {
      write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<ROOT LABEL=\"TWEET\">")

      tweetDocs.foreach(doc => {
        write(doc.toXML())
      })

      write("</ROOT>")
      close()
    }
  }


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
          if (authorDelete(authorText)) {
            del = del :+ filepath
          }


        }
      }
      catch {
        case sax: SAXParseException => {
          seq = seq :+ (filename, sax.getMessage, sax.getLineNumber, sax.getColumnNumber)
          del = del :+ filepath
        }
      }
    })

    seq.foreach(tuple => {

      println("Filename: " + tuple._1 + " Line:" + tuple._3, " Column:" + tuple._4 + "Message: " + tuple._2)

    })

    println("Deleting " + del.length + " files from folder")
    println("Delete all files? Y/N")
    val line = Console.readLine()
    if(line.trim.equals("Y")) {
      del.foreach(filename => {
        println("Deleting filename : " + filename)
        val file = new File(filename)
        if (file.exists()) {
          file.delete();
        }
      })
    }

  }


  def main(args: Array[String]) {

    XMLParser.fixFolder(Resources.DocumentResources.articleDir)
    XMLParser.fixFolder(Resources.DocumentResources.blogDir)
    //XMLParser.fixTweets(Resources.DocumentResources.twitterDir, "twitter-main-0.xml")
    //XMLParser.checkFolder(Resources.DocumentResources.blogDir)

  }
}

