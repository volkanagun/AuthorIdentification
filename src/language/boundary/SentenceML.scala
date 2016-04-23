package language.boundary

import java.io.{IOException, _}
import java.nio.charset.Charset

import opennlp.tools.sentdetect._
import opennlp.tools.util.{ObjectStream, PlainTextByLineStream, TrainingParameters}
import options.Resources

/**
  * Created by wolf on 05.04.2016.
  */
class SentenceML extends Serializable{
  val modelFilename = Resources.OpenSentenceBD.modelFilename
  val sentenceDetector = new SentenceDetectorME(new SentenceModel(new File(modelFilename)))

  def fit(text: String): Array[String] = {
    this.synchronized{
      return sentenceDetector.sentDetect(text)
    }
  }

  @throws[IOException]
  def train(filename: String) {
    val charset: Charset = Charset.forName("UTF-8")
    val lineStream: ObjectStream[String] = new PlainTextByLineStream(new FileInputStream(filename), charset)
    val sampleStream: ObjectStream[SentenceSample] = new SentenceSampleStream(lineStream)
    val sentenceDetectorFactory: SentenceDetectorFactory = new SentenceDetectorFactory
    var model: SentenceModel = null
    try {
      val detectorFactory: SentenceDetectorFactory = new SentenceDetectorFactory
      model = SentenceDetectorME.train("tr", sampleStream, detectorFactory, TrainingParameters.defaultParams)
    } finally {
      sampleStream.close
    }
    var modelOut: OutputStream = null
    try {
      modelOut = new BufferedOutputStream(new FileOutputStream(modelFilename))
      model.serialize(modelOut)
    } finally {
      if (modelOut != null) modelOut.close
    }
  }
}

object SentenceML{
  val sentenceML = new SentenceML

  def main(args: Array[String]) {
    sentenceML.fit("Ali geldi. Gitti. Gördü.")
      .foreach(sentence=>{println(s"Sentence:$sentence")})
  }
}
