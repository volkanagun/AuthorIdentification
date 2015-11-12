package opennlp.models

import java.io.FileInputStream

import opennlp.tools.cmdline.parser.ParserTool
import opennlp.tools.parser.{ParserFactory, chunking, ParserModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import scala.collection.mutable._


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

  def fit(sentence: String): String = {
    //Number of Parses, Average Depth
    //Top parse sub sentence count
    //Top parse depth of each parse head
    //Top parse top NP,VP,S lengths
    val topParses = ParserTool.parseLine(sentence, parser, 10)
    val parse = topParses(0)
    val buffer = new StringBuffer()
    //parse.showCodeTree(buffer)
    parse.show(buffer)
    buffer.toString

  }
}

class Node(var cat: String, var value: String, var elements: List[Node]) {
  def this() = this(null, null, List())

  def add(element: Node): Node = {
    elements = elements.:+(element)
    this
  }

  def isEmpty(): Boolean = {
    return elements.isEmpty
  }

  def emptyCat(): Boolean = {
    return cat == null
  }

  def text(): String = {
    if (elements.isEmpty) {
      value
    }
    else {
      return elements.foldLeft[String](new String("")) {
        (txt, node) => {
          txt +" "+ node.text()
        }
      }.trim
    }
  }

  override def toString(): String = {
    var txt = "(" + cat
    if (value != null && !value.isEmpty) {
      txt += " " + value
    };

    elements.foreach(element => {
      txt += " " + element.toString()
    })
    txt + ")"
  }
}


object SyntaxML {
  def parse(syntax: String): Node = {
    val stack = Stack[Node]()
    val chars = syntax.toCharArray
    var crrnt = ""
    var topNode: Node = null;
    chars.foreach(char => {

      if (char == '(') {
        topNode = new Node()
        stack.push(topNode)
      }
      else if (char == ')') {
        val newNode = stack.pop
        if (!stack.isEmpty) {
          topNode = stack.head
          topNode.add(newNode)
          newNode.value = crrnt
          crrnt = ""
        }
        else {
          topNode = newNode
        }
      }
      else if (char != ' ') {
        crrnt += char
      }
      else if (char == ' ' && topNode.emptyCat()) {
        topNode = stack.head
        topNode.cat = crrnt
        crrnt = ""
      }


    })

    topNode
  }

  def main(args: Array[String]) {
    val ml = new SyntaxML
    //val syntax = ml.fit("")
    val syntax = ml.fit("Did I give the money to the burglar...")
    val node = parse(syntax)
    println(node)
    println(node.text())
    println(syntax)
  }
}
