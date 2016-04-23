package org.apache.spark.ml.feature.extraction

import options.{Dictionary, Resources}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.io.Source

/**
 * Created by wolf on 30.10.2015.
 */
class EmoticonDetector(override val uid: String) extends UnaryTransformer[Seq[String], Seq[Seq[String]], EmoticonDetector] {

  val filename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/dictionary/emoticons/emoticons.txt"
  lazy val emoticons = Dictionary.loadEmoticons()

  def this() = this(Identifiable.randomUID("emoticons"))


  def print(): Unit ={
    emoticons.foreach(p=>{
      println(p._1+"\t"+p._2)
    })
  }

  override protected def createTransformFunc: (Seq[String]) => Seq[Seq[String]] = {
    emoticonFinder(_)
  }

  def emoticonFinder(sentences: Seq[String]): Seq[Seq[String]] = {
    val iter = emoticons.toSeq
    var emoset = Seq[Seq[String]]()
    sentences.foreach(sentence => {
      val filter = iter.filter(pair=>sentence.contains(pair._1))
      emoset = emoset :+ filter.map(pair=>pair._2)
    })
    emoset
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)), s"Input type must be ArrayType[StringType] but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(new ArrayType(StringType, false),false)

}

object EmoticonDetector{
  def main(args: Array[String]) {
    val emo = new EmoticonDetector()
    emo.print()
  }
}
