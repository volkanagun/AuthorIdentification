package language.postagger

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.charset.Charset

import opennlp.tools.postag.{POSModel, POSTaggerME, WordTagSampleStream}
import opennlp.tools.util.{PlainTextByLineStream, TrainingParameters}

/**
 * Created by wolf on 05.11.2015.
 */
class PosTaggerML extends Serializable{
  val modelFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/training/en-pos.bin";
  lazy val posTagger:POSTaggerME = {
    val in = new FileInputStream(modelFilename)
    val model = new POSModel(in)
    new POSTaggerME(model)
  }

  def fit(tokens:Array[String]): Array[String] ={
    posTagger.tag(tokens)
  }

  def train(filename:String): Unit ={
    val lineStream = new PlainTextByLineStream(new FileInputStream(filename),Charset.forName("UTF-8"));
    val sampleStream = new WordTagSampleStream(lineStream)
    val model = POSTaggerME.train("en", sampleStream, TrainingParameters.defaultParams(), null, null);
    sampleStream.close()
    save(model)
  }


  def save(model:POSModel): Unit ={
    val modelOut = new BufferedOutputStream(new FileOutputStream(modelFilename))
    model.serialize(modelOut)
    modelOut.close()
  }

}
