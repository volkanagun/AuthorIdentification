package org.apache.spark.ml.feature

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, ArrayType, DataType}

import scala.io.Source

/**
 * Created by wolf on 30.10.2015.
 */
class EmoticonDetector(override val uid: String) extends UnaryTransformer[Seq[String], Seq[Seq[String]], EmoticonDetector] {

  val filename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/dictionary/emoticons/emoticons.txt"
  lazy val emoticons = load()

  def this() = this(Identifiable.randomUID("emoticons"))

  def load(): Map[String, String] = {
    val text = Source.fromFile(filename).mkString
    var map: Map[String, String] = Map()
    text.split("\n").foreach(line => {
      val split = line.split("\t")
      val emoticonList = split(0).split("\\s+")
      val emoticonMeaning = split(1).split("\\,(\\s+?)")

      emoticonList.foreach(emo => {
        map = map.+((emo, emoticonMeaning(0)))
      })
    })

    map
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
