package data.dataset

import data.document
import options.Resources.DocumentModel
import processing.structures.docs._

import scala.collection.immutable.Stack
import scala.util.control.Breaks

/**
  * Created by wolf on 07.01.2016.
  */
class XYZParser extends Serializable {

  def parseDocument(filename: String, text: String): Seq[document.Document] = {
    if (text.contains("<ROOT LABEL=\"B\">")) {
      parseBlog(filename, text)
    }
    else if ((text.contains("<ROOT LABEL=\"A\">"))) {
      parseArticle(filename, text)
    }
    else {
      parseTweet(filename, text)
    }
  }

  def parseBlog(filename: String, text: String): Seq[document.Document] = {
    val tag = "RESULT"
    val attrArticle = Array[String]("TYPE", "CONTAINER", "LABEL", "ARTICLE")
    val attrAuthor = Array[String]("TYPE", "TEXT", "LABEL", "AUTHORNAME")
    val attrGenre = Array[String]("TYPE", "TEXT", "LABEL", "GENRE")
    val attrTitle = Array[String]("TYPE", "TEXT", "LABEL", "ARTICLETITLE")
    val attrText = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLETEXT")
    val attrParagraph = Array[String]("TYPE", "TEXT", "LABEL", "ARTICLEPARAGRAPH")

    val seq = Seq[document.Document]()
    val (articleContainer, i1) = toText(0, text, tag, attrArticle)
    val (author, i2) = toText(0, articleContainer, tag, attrAuthor)
    val (genre, i5) = toText(0, articleContainer, tag, attrGenre)
    val (title, i3) = toText(0, articleContainer, tag, attrTitle)
    val (content, i4) = toText(0, articleContainer, tag, attrText)
    var pindex = 0
    val blog = new document.Document(filename, DocumentModel.BLOGDOC, author.trim)
    blog.setGenre(genre.trim)
    blog.setText(toTagClean(content))
    blog.setTitle(title.trim)
    while (pindex >= 0) {

      val (paragraph, index) = toText(pindex, content, tag, attrParagraph)
      pindex = index

      if (pindex > 0) {
        blog.addParagraph(toControlTrim(paragraph))
      }
    }



    seq :+ blog
  }

  def parseArticle(filename: String, text: String): Seq[document.Document] = {
    val tag = "RESULT"
    val attrArticle = Array[String]("TYPE", "ARTICLE", "LABEL", "CONTAINER")
    val attrAuthor = Array[String]("TYPE", "ARTICLE", "LABEL", "AUTHORNAME")
    val attrGenre = Array[String]("TYPE", "ARTICLE", "LABEL", "GENRE")
    val attrTitle = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLETITLE")
    val attrText = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLETEXT")
    val attrParagraph = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLEPARAGRAPH")

    val seq = Seq[document.Document]()
    val (articleContainer, i1) = toText(0, text, tag, attrArticle)
    val (author, i2) = toText(0, articleContainer, tag, attrAuthor)
    val (genre, i5) = toText(0, articleContainer, tag, attrGenre)
    val (title, i3) = toText(0, articleContainer, tag, attrTitle)
    val (content, i4) = toText(0, articleContainer, tag, attrText)
    var pindex = 0
    val article = new document.Document(filename, DocumentModel.ARTICLEDOC, author.trim)
    article.setGenre(genre.trim)
    article.setText(toTagClean(content))
    article.setTitle(title.trim)


    while (pindex >= 0) {

      val (paragraph, index) = toText(pindex, content, tag, attrParagraph)
      pindex = index

      if (pindex > 0) {
        article.addParagraph(paragraph)
      }
    }

    seq :+ article
  }

  def parseArticles(filename: String, text: String): Seq[document.Document] = {
    val tag = "RESULT"
    val attrArticle = Array[String]("TYPE", "ARTICLE", "LABEL", "CONTAINER")
    val attrAuthor = Array[String]("TYPE", "ARTICLE", "LABEL", "AUTHORNAME")
    val attrGenre = Array[String]("TYPE", "ARTICLE", "LABEL", "GENRE")
    val attrTitle = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLETITLE")
    val attrText = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLETEXT")
    val attrParagraph = Array[String]("TYPE", "ARTICLE", "LABEL", "ARTICLEPARAGRAPH")

    var seq = Seq[document.Document]()
    var sindex = 0
    var count = 0

    while (sindex >= 0) {
      val (articleContainer, i1) = toText(sindex, text, tag, attrArticle)
      val (author, i2) = toText(0, articleContainer, tag, attrAuthor)
      val (genre, i5) = toText(0, articleContainer, tag, attrGenre)
      val (title, i3) = toText(0, articleContainer, tag, attrTitle)
      val (content, i4) = toText(0, articleContainer, tag, attrText)
      var pindex = 0
      if (i1 > 0) {
        val article = new document.Document(filename, DocumentModel.ARTICLEDOC, author.trim)
        article.setGenre(genre.trim)
        article.setText(toTagClean(content))
        article.setTitle(title.trim)


        while (pindex >= 0) {

          val (paragraph, index) = toText(pindex, content, tag, attrParagraph)
          pindex = index

          if (pindex > 0) {
            article.addParagraph(paragraph)
          }
        }

        seq = seq :+ article
      }

      sindex = i1
    }


    seq
  }


  def parseTweet(filename: String, text: String): Seq[document.Document] = {
    val attrTweet = Array[String]("TYPE", "CONTAINER", "LABEL", "TWEET")
    val attrAuthor = Array[String]("TYPE", "TEXT", "LABEL", "AUTHORNAME")
    val attrText = Array[String]("TYPE", "TEXT", "LABEL", "TWEETTEXT")

    var seq = Seq[document.Document]()
    var sindex = 0
    var count = 0
    val breaks = new Breaks
    breaks.breakable {
      while (sindex >= 0) {

        val (tweetContainer, lastIndex) = toText(sindex, text, "RESULT", attrTweet)
        val (authorText, authorIndex) = toText(0, tweetContainer, "RESULT", attrAuthor)
        val (tweetText, tweetIndex) = toText(0, tweetContainer, "RESULT", attrText)

        if (lastIndex > 0) {
          val author = authorText.trim
          val tweet = tweetText.trim
          val document = new data.document.Document(filename + "-" + count, DocumentModel.TWITTERDOC, author)
          document.setText(tweet)
          document.setGenre(DocumentModel.TWITTER)
          seq = seq :+ document
          count = count + 1
        }

        sindex = lastIndex
      }
    }

    seq
  }

  protected def toText(sindex: Int, text: String, tag: String, attributes: Array[String]): (String, Int) = {
    val startTag = toOpen(tag) + " " + toText(attributes)
    val openTag = toOpen(tag)
    val closeTag = toClose(tag)
    val stack = Stack[String]()

    val index = text.indexOf(startTag, sindex)

    if (index == -1) return ("", -1)

    var startIndex = index + startTag.length
    var outIndex = -1
    val inIndex = text.indexOf(">", startIndex) + 1
    var openCount = 1

    val break = new Breaks
    var closeIndex = 0
    var preCloseIndex = startIndex
    var openIndex = 0
    var nextOpenIndex = -1
    var preOpenIndex = startIndex


    break.breakable {
      while (index > -1) {


        closeIndex = text.indexOf(closeTag, startIndex)
        openIndex = text.indexOf(openTag, startIndex)
        nextOpenIndex = text.indexOf(openTag, openIndex + openTag.length)

        if (openCount > 0 && openIndex > 0 && nextOpenIndex > openIndex && nextOpenIndex < closeIndex) {
          startIndex = nextOpenIndex
          openCount += 1
        }
        else if (openCount > 0 && openIndex > preOpenIndex && openIndex < closeIndex) {
          startIndex = closeIndex + closeTag.length
        }
        else if (openCount > 0 && closeIndex > preOpenIndex) {
          openCount -= 1
          startIndex = closeIndex + closeTag.length
        }
        else if (openCount > 0 && openIndex > closeIndex) {

        }

        preCloseIndex = closeIndex
        preOpenIndex = openIndex

        if (openCount == 0) {
          outIndex = closeIndex
          break.break()
        }


      }
    }

    if (inIndex > 0 && outIndex > inIndex) {
      (text.substring(inIndex, outIndex), outIndex)
    }
    else {
      ("", -1)
    }
  }


  protected def toText(attributes: Array[String]): String = {
    var attributeText = ""
    for (i <- 0 until attributes.length - 1 by 2) {
      attributeText += attributes(i) + "=\"" + attributes(i + 1) + "\" ";

    }

    attributeText.trim
  }

  def toOpen(tag: String): String = {
    "<" + tag
  }

  def toClose(tag: String): String = {
    "</" + tag + ">"
  }

  protected def toTagClean(text: String): String = {
    val regex = "<\\/?RESULT.*?>\n"
    return text.replaceAll(regex, "").trim
  }

  protected def toControlTrim(text: String): String = {
    var result = text
    if (text.startsWith("\n")) {
      result = text.substring(1);

    }

    if (text.endsWith("\n")) {
      result = text.substring(0, text.length - 1)
    }

    result

  }

}

object XYZParser extends Serializable {
  val parser = new XYZParser

  def parse(filename: String, text: String): Array[document.Document] = {
    parser.parseDocument(filename, text).toArray[document.Document]
  }

  def main(args: Array[String]) {
    val parser = new XYZParser
    val textBlog = "<RESULT TYPE=\"CONTAINER\" LABEL=\"ARTICLE\">\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLETITLE\">\nDevrim Demirel'den Program İndirme Platformu Tamindir.com'a Yatırım\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"GENRE\">\nyatirim\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\nArda Kutsal\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETEXT\">\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nBeriltech'in kurucusu Devrim Demirel'in Türkiye'nin en popüler program indirme sitelerinden Tamindir.com'a ortak olduğunu öğrendik.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nİzmir merkezli şirkete gerçekleşen yatırımla ilgili aldığımız bilgilere göre 1.3 milyon dolara değerlenen Tamindir.com'un %10 hissesi Devrim Demirel'e geçmiş.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nKonuyla ilgili Devrim Demirel'le görüştüm ve hem bize Tamindir.com'la ilgili ek bilgiler aktardı hem de yatırım haberini doğruladı.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nSektörün e-ticaret başta olmak üzere hareketli olduğu alanları göz önüne alacak olursak, sessiz sakin şekilde ilerleyen bir kulvarda hayatını sürdüren Tamindir.com'a gerçekleşen yatırım alıştığımızın dışında bir gelişme olduğu için şahsen şaşırmıştım. Ancak sonrasında şirketle ilgili detayları öğrendiğimde geçtiğimiz günlerde paylaştığım gibi gözden kaçan alanlara da dikkat etmek gerektiğine bir kez daha emin oldum.\n</RESULT>\n\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\n2010 yılında 65 milyon tekil ziyaretçiye ulaşan site, 2011'in ilk yarısında ise 42 milyon tekil ziyaretçiye ulaşmış. Sitede her ay 1 milyon arama yapıldığı da yatırım haberi sonrasında aldığımız bilgiler arasında.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nGoogle verilerine göre Türkiye'nin en çok ziyaret edilen 42. sitesi olan Tamindir.com, lisanslı yazılım dağıtımı ve satışı yapıyor. Birçok yazılım ve oyun üreticisinin Türkiye'deki indirme merkezi konumundaki siteden haftada 2 milyondan fazla program indiriliyor. Söz konusu dosya indirmelerin %20'sinin Tamindir.com'un kendi sunucularından gerçekleştiğini de paylaşmış olalım.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nEk olarak güzel bir veri olduğu için belirteyim, şirketten TTNET raporlarına göre Türkiye'de web üzerinden gerçekleşen günlük dosya indirmelerin %9'unun Tamindir.com üzerinden yapıldığını da öğrendik.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nGerçekleştirdiği yatırımlarda Beriltech'le yaratılacak sinerjiyi ön plana çıkartmayı hedefleyen Devrim Demirel, bir süre önce Botégo'ya da ortak olmuştu. Semantik teknolojilere odaklanan şirket arama motoru Şirketçe'yi bünyesinde barındıran Beriltech, aynı zamanda bir süre önce Kimdir.com'u da hayata geçirmişti.\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"ARTICLEPARAGRAPH\">\nYazımın başında da belirttiğim gibi e-ticaret ve online oyun hareketliliğinde gözümüzden kaçırdığımız bir alanda hizmet vermekte olan Tamindir.com'un istatistikleri ve değerlemesi oldukça dikkatimi çekti. Bu gelişmenin de ispatladığı gibi, Türkiye internet pazarında girişimci ve yatırımcıların radarlarını biraz genişletmeleri ve farklı alanlara da kesinlikle odaklanmaları gerekiyor.\n</RESULT>\n</RESULT>\n</RESULT>"
    val textArticle = "<RESULT TYPE=\"ARTICLE\" LABEL=\"CONTAINER\">\n<RESULT TYPE=\"ARTICLE\" LABEL=\"AUTHORNAME\">\nKazım Güleçyüz | Yeni Asya\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETITLE\">\nSuriye çıkmazı\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"GENRE\">\npolitics\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETEXT\">\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nSuriye'de gelinen nokta, Türkiye'nin bu ülkeyle ilgili olarak uyguladığı politikanın sorgulanması ve değiştirilmesi zaruretini çok daha kuvvetli ve vurgulu bir şekilde gündeme taşımış bulunuyor.Şimdiye kadar izlenen, \"Esed'e muhalif grupları destekleme\" siyaseti, ülkedeki çatışma ortamını iyice kızıştırdı ve meydan, IŞİD gibi, dizgini ve kumandası uluslararası şer komitelerinin elinde olan vahşi terör örgütlerine kaldı.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nKeza PKK'nın Suriye kolu olarak bilinen yapılanmalara gün doğdu.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nİş öyle bir hale geldi ki, Suriye sınırımızın hemen altındaki Kobani ve Tel Abyad gibi yerleşim merkezleri IŞİD'le PYD arasındaki hakimiyet mücadelesinin arenalarına dönüştü.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nBuralardaki çatışmaların bizdeki yansımaları ise ciddi iç güvenlik endişeleri doğurdu.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nGeçen güz Kobani, IŞİD kontrolüne geçtiğinde, orada alevlenen kanlı fitne ateşinin hem de Kurban Bayramı günlerinde ülkemizi de nasıl yaktığını çok acı bir şekilde gördük ve yaşadık.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nŞimdi yine Kobani-IŞİD ve Tel Abyad-PYD olayları sebebiyle diken üstündeyiz.\n</RESULT>\n</RESULT>\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"CONTAINER\">\n<RESULT TYPE=\"ARTICLE\" LABEL=\"AUTHORNAME\">\nKazım Güleçyüz | Yeni Asya\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETITLE\">\nSuriye çıkmazı\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"GENRE\">\npolitics\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLETEXT\">\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nSuriye'de gelinen nokta, Türkiye'nin bu ülkeyle ilgili olarak uyguladığı politikanın sorgulanması ve değiştirilmesi zaruretini çok daha kuvvetli ve vurgulu bir şekilde gündeme taşımış bulunuyor.Şimdiye kadar izlenen, \"Esed'e muhalif grupları destekleme\" siyaseti, ülkedeki çatışma ortamını iyice kızıştırdı ve meydan, IŞİD gibi, dizgini ve kumandası uluslararası şer komitelerinin elinde olan vahşi terör örgütlerine kaldı.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nKeza PKK'nın Suriye kolu olarak bilinen yapılanmalara gün doğdu.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nİş öyle bir hale geldi ki, Suriye sınırımızın hemen altındaki Kobani ve Tel Abyad gibi yerleşim merkezleri IŞİD'le PYD arasındaki hakimiyet mücadelesinin arenalarına dönüştü.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nBuralardaki çatışmaların bizdeki yansımaları ise ciddi iç güvenlik endişeleri doğurdu.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nGeçen güz Kobani, IŞİD kontrolüne geçtiğinde, orada alevlenen kanlı fitne ateşinin hem de Kurban Bayramı günlerinde ülkemizi de nasıl yaktığını çok acı bir şekilde gördük ve yaşadık.\n</RESULT>\n<RESULT TYPE=\"ARTICLE\" LABEL=\"ARTICLEPARAGRAPH\">\nŞimdi yine Kobani-IŞİD ve Tel Abyad-PYD olayları sebebiyle diken üstündeyiz.\n</RESULT>\n</RESULT>\n</RESULT>"
    val textTweet = "<RESULT TYPE=\"CONTAINER\" LABEL=\"TWEET\">\n<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\ncüneyt özdemir\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"TWEETTEXT\">\n'Düşler Akademisi' Türkiyenin gururu bir sosyal girişim... Var edenlere de destekleyenlere de bravo...\n</RESULT>\n</RESULT>\n<RESULT TYPE=\"CONTAINER\" LABEL=\"TWEET\">\n<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\nArdan Zentürk\n</RESULT>\n<RESULT TYPE=\"TEXT\" LABEL=\"TWEETTEXT\">\nHAARETZ: Türk-İsrail mutabakatını Mısır engelledi, Gazze ablukasının devamını istedi. Detayı 23.00 @ModeratorGece24 @yirmidorttv\n</RESULT>\n</RESULT>"
    val attr = Array[String]("TYPE", "CONTAINER", "LABEL", "TWEET")
    val seqArticle = parser.parseArticles("", textArticle)
    val seqTweet = parser.parseTweet("", textTweet)
    val seqBlog = parser.parseBlog("", textBlog)



    seqBlog.foreach(a => {
      println("Author:" + a.getAuthor + " Text:" + a.getText)
    })

    seqTweet.foreach(a => {
      println("Author:" + a.getAuthor + " Text:" + a.getText)
    })

    seqArticle.foreach(a => {
      println("Author:" + a.getAuthor + " Text:" + a.getText)
    })

  }
}
