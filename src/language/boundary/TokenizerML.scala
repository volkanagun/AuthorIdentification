package language.boundary

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.charset.Charset

import opennlp.tools.tokenize.{TokenSampleStream, TokenizerFactory, TokenizerME, TokenizerModel}
import opennlp.tools.util.{PlainTextByLineStream, TrainingParameters}
import options.Resources


/**
 * Created by wolf on 28.10.2015.
 */
class TokenizerML extends Serializable
{
  val modelFilename = Resources.OpenTokenizer.modelFilename

  lazy val tokenizer: TokenizerME = {
    val in = new FileInputStream(modelFilename)
    val model = new TokenizerModel(in)
    new TokenizerME(model)
  }

  def fit(sentence: String): Vector[String] = {
    var tokens = Vector[String]()
    tokenizer.tokenize(sentence).foreach(token=>tokens = tokens :+ token)
    tokens
  }

  def train(filename:String): Unit ={
    val lineStream = new PlainTextByLineStream(new FileInputStream(filename),Charset.forName("UTF-8"));
    val sampleStream = new TokenSampleStream(lineStream)
    val tokenizerFactor = new TokenizerFactory("tr",null,false, null)
    val model = TokenizerME.train(sampleStream, tokenizerFactor, TrainingParameters.defaultParams())
    sampleStream.close()
    save(model)
  }

  def save(model:TokenizerModel): Unit ={
    val modelOut = new BufferedOutputStream(new FileOutputStream(modelFilename))
    model.serialize(modelOut)
    modelOut.close()
  }

}

object TokenizerTest{
  def main(args:Array[String]): Unit ={
    val tokenizerScala = new TokenizerML();
    println(tokenizerScala.fit("Ali cümle kurdu."))
  }
}
