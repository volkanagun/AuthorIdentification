package language.boundary

import java.io.FileInputStream

import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.util.Span
import options.Resources

/**
 * Created by wolf on 07.11.2015.
 */
class NamedEntityML {

  val modelPersonFilename = Resources.OpenNER.modelPersonFilename
  val modelLocationFilename = Resources.OpenNER.modelLocationFilename
  val modelTimeFilename = Resources.OpenNER.modelTimeFilename
  val modelPercentageFilename = Resources.OpenNER.modelPercentageFilename
  val modelMoneyFilename = Resources.OpenNER.modelMoneyFilename
  val modelOrganizationFilename = Resources.OpenNER.modelOrganizationFilename
  val modelDateFilename = Resources.OpenNER.modelDateFilename

  val modelFilenames = Array(modelPersonFilename, modelLocationFilename, modelTimeFilename, modelPercentageFilename, modelMoneyFilename, modelOrganizationFilename, modelDateFilename)

  lazy val nerTaggers: Array[NameFinderME] = {
    var taggers = Array[NameFinderME]()
    modelFilenames.foreach(filename => {
      val in = new FileInputStream(filename)
      val model = new TokenNameFinderModel(in)
      taggers = taggers :+ new NameFinderME(model)
    })
    taggers
  }

  protected def fit(nerTagger: NameFinderME, tokens: Array[String]): Array[Span] = {
    nerTagger.find(tokens)
  }

  protected def merge(tokens: Array[String]): Array[Span] = {
    var spans = Array[Span]()
    nerTaggers.foreach(tagger => {
      spans = spans ++ fit(tagger, tokens)
    })

    spans.sorted
  }

  protected def tag(tokens: Array[String]): String = {
    val spans = merge(tokens)
    var sentence = ""
    var index = 0
    spans.foreach(span => {
      val start = span.getStart
      val end = span.getEnd
      for (i <- index until start) sentence += " "+tokens(i)
      sentence += " "+"<" + span.getType + ">"
      index = span.getEnd
    })

    for(i<-index until tokens.length) sentence+=" "+tokens(i)
    sentence.trim
  }

  def fit(tokens: Array[String]): String = {
    tag(tokens)
  }
}

object NamedEntityML {
  val personEntity = "<peron>"
  val locationEntity = "<location>"
  val timeEntity = "<time>"
  val organizationEntity = "<organization>"
  val dateEntity = "<date>"
  val moneyEntity = "<moeny>"
  val percentageEntity = "<percentage>"

  def main(args: Array[String])
  {
    val ml = new NamedEntityML()
    println(ml.tag(Array("Spencer", "arrived", "to", "the", "Ford", "Motor", "Company", "from", "Chicago")))
  }
}
