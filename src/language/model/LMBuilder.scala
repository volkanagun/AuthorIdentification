package language.model

import java.io.FileWriter


import edu.berkeley.nlp.lm.io.{MakeKneserNeyArpaFromText, MakeLmBinaryFromArpa}
import language.tokenization.MyTokenizer
import options.Resources

import scala.io.Source

/**
  * Created by wolf on 31.03.2016.
  */
class LMBuilder(val sourceFile: String, val destinationFile: String,
                val ngram:Int) {

  val myTokenizer = new MyTokenizer()

  def build(): Unit = {
    //read sentences in lines
    //find stop points
    val fw = new FileWriter(destinationFile, false)
    var count = 0
    for(line <- Source.fromFile(sourceFile).getLines())
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

      count = count + 1
      println(s"Line $count")
    }

    fw.close()
  }

  def arpa(arpaFilename:String) : Unit = {
    //5 kneserNeyFromText.arpa ../test/edu/berkeley/nlp/lm/io/big_test.txt
    val array = Array(ngram.toString, arpaFilename, destinationFile)
    MakeKneserNeyArpaFromText.main(array)

  }

  def binary(arpaFilename:String, binaryFilename:String): Unit ={
    val array = Array(arpaFilename,binaryFilename)
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
  val smallSentences = Resources.LanguageModel.sentencesSmall
  val largeSentences = Resources.LanguageModel.sentencesLarge
  val ngramSentences = Resources.LanguageModel.modelLMNgram
  val arpaBinary = Resources.LanguageModel.modelLM5TRArpaFilename
  val lmBinary = Resources.LanguageModel.modelLM5TRFilename

  val prepare = new LMBuilder(largeSentences,ngramSentences, 5)

  def main(args: Array[String]) {
    prepare.build()
    prepare.arpa(arpaBinary)
    prepare.binary(arpaBinary,lmBinary)
  }

}
