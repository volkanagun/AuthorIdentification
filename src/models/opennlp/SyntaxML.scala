package models.opennlp

import java.io.FileInputStream

import opennlp.tools.cmdline.parser.ParserTool
import opennlp.tools.parser.{ParserFactory, chunking, ParserModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}

/**
 * Created by wolf on 05.11.2015.
 */
class SyntaxML {
  val modelFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/training/en-chunking.bin";
  lazy val parser = {
    val in = new FileInputStream(modelFilename)
    val model = new ParserModel(in)
    ParserFactory.create(model)
  }

  def fit(sentence:String): Array[String] ={
    val topParses = ParserTool.parseLine(sentence,parser,10)
    topParses(0).showCodeTree()
    Array()
  }
}

object SyntaxML{
  def main(args: Array[String]) {
    val ml = new SyntaxML
    ml.fit("I ran the attatched sheet, and the last group of deals that I was given had old deals blended with new deals.")
  }
}
