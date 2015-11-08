package opennlp.models

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
    //Number of Parses, Average Depth
    //Top parse sub sentence count
    //Top parse depth of each parse head
    //Top parse top NP,VP,S lengths
    val topParses = ParserTool.parseLine(sentence,parser,10)
    val parse = topParses(0)
    val buffer = new StringBuffer()
    parse.showCodeTree(buffer)
    println(buffer.toString)
    Array()
  }
}

object SyntaxML{
  def main(args: Array[String]) {
    val ml = new SyntaxML
    ml.fit("I ran the attatched sheet, and the last group of deals that I was given had old deals blended with new deals.")
  }
}
