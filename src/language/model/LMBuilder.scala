package language.model

import java.io
import java.io.{File, FileWriter}

import edu.berkeley.nlp.lm.io.{MakeKneserNeyArpaFromText, MakeLmBinaryFromArpa}
import language.tokenization.MyTokenizer
import options.Resources

import scala.io.Source
import scala.util.Random

/**
  * Created by wolf on 31.03.2016.
  */
class LMBuilder(val ngram:Int) {

  val myTokenizer = new MyTokenizer()


  def create(text:String, filename:String): this.type ={
    if(new File(filename).exists()) return this
    if(filename==null || text.trim.isEmpty) return this

    val tmpNgram = "/tmp/" + Random.nextString(30)
    val tmpArpa = "/tmp/" + Random.nextString(30)


    val lines = text.split("\n").toIterator
    build(lines, tmpNgram)
    arpa(tmpNgram,tmpArpa)
    binary(tmpArpa,filename)

    new File(tmpNgram).delete()
    new File(tmpArpa).delete()

    this
  }

  def build(lines:Iterator[String], destinationFile:String):Boolean ={
    val fw = new FileWriter(destinationFile, false)
    val written = false
    for(line <- lines)
    {
      val skip = false
      val tokens = myTokenizer.tokenize(line)
      var seq = Seq[String]()
      for(token <- tokens){
        if(isValid(token)){
          seq = seq :+token
        }
        else if(isSkip(token) && seq.length>2){
          fw.write(seq.mkString(" ")+"\n")
          seq = Seq[String]()
        }
        else if(seq.length>2){
          fw.write(seq.mkString(" ")+"\n")
          seq = Seq[String]()
        }
        else{
          seq = Seq[String]()
        }
      }

      fw.write(seq.mkString(" ")+"\n")
    }


    fw.close()

    written
  }

  def build(sourceFile: String, destinationFile: String): Unit = {
    //read sentences in lines
    //find stop points

    var count = 0
    val lines = Source.fromFile(sourceFile).getLines()
    build(lines,destinationFile)

  }

  def arpa(sourceFilename:String, arpaFilename:String) : Unit = {
    //5 kneserNeyFromText.arpa ../test/edu/berkeley/nlp/lm/io/big_test.txt
    val array = Array(ngram.toString, arpaFilename, sourceFilename)
    MakeKneserNeyArpaFromText.main(array)

  }

  def binary(arpaFilename:String, binaryFilename:String): Unit ={
    val array = Array(arpaFilename, binaryFilename)
    MakeLmBinaryFromArpa.main(array);
  }

  def isValid(word: String): Boolean = {
    val alllower = word.forall(char => {
      (char.isLower)
    })

    !word.isEmpty && alllower
  }

  def isSkip(word:String):Boolean ={
    word.equals("'")
  }

}

object LMBuilder{
  val smallSentences = Resources.LMResource.sentencesSmall
  val largeSentences = Resources.LMResource.sentencesLarge
  val ngramSentences = Resources.LMResource.modelLMNgram
  val arpaBinary = Resources.LMResource.modelLM5TRArpaFilename
  val lmBinary = Resources.LMResource.modelLM5TRFilename

  val prepare = new LMBuilder(5)

  def main(args: Array[String]) {
    prepare.build(largeSentences,ngramSentences)
    prepare.arpa(ngramSentences, arpaBinary)
    prepare.binary(arpaBinary,lmBinary)
  }

}
