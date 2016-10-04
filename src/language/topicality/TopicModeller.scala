package language.topicality

import java.io.PrintWriter
import java.net.URI

import cc.mallet.pipe.{CharSequence2TokenSequence, Pipe, SerialPipes, TokenSequence2FeatureSequence}

import cc.mallet.types._
import cc.mallet.util.Randoms
import options.{Parameters, Resources}

import scala.util.Random

/**
  * Created by wolf on 08.09.2016.
  */
class TopicModeller(val numIter:Int) extends Serializable {

  val pipe = new TopicPipe()
  val hLDA = new HLDA()

  protected def instance(document: Seq[String], id: String): Instance = {
    val uri = new URI(id)
    new Instance(document, null, uri, null)
  }

  protected def instance(document: Seq[String]): Instance = {
    val rndInt = new Random().nextInt()
    instance(document, rndInt.toString)
  }

  protected def instances(documents: Seq[(String,Seq[String])]): InstanceList = {
    val instanceList = new InstanceList(pipe)
    documents.foreach{case(id,document) => {
      val docInstance = instance(document, id)
      instanceList.addThruPipe(docInstance)
    }}

    instanceList
  }

  def train(documents:Seq[(String, Seq[String])]): Unit ={
    hLDA.initialize(instances(documents),null, 4, new Randoms())
    hLDA.estimate(numIter)
    hLDA.printState(new PrintWriter("hlda-state.txt"))
  }
}

class TopicModel(hLDA:HLDA) extends Serializable{

}

object TopicModeller{
  val modeller = new TopicModeller(1000)
  def createModel(documents:Seq[(String, Seq[String])]): Unit ={

    modeller.train(documents)

  }
}

class TopicPipe extends Pipe(new Alphabet(), null) {

  setTargetProcessing(false)



  override def pipe(instance: Instance): Instance = {
    extract(instance)
  }

  def extract(instance: Instance): Instance = {
    val tokens = instance.getData.asInstanceOf[Seq[String]]


    /*val indices = tokens.map(token => featureAlphabet.lookupIndex(token))
      .filter(indice=> indice >= 0)
      .toArray*/

    val sequence = new FeatureSequence(getDataAlphabet, tokens.size)
    tokens.foreach(token => {
      val lowerCase = token.toLowerCase(Parameters.crrLocale)
      sequence.add(lowerCase)
    })

    instance.setData(sequence)
    instance
  }

}