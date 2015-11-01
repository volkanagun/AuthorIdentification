package org.apache.spark.ml.feature


import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ParamValidators, IntParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, StringType, DataType}

/**
 * Created by wolf on 30.10.2015.
 */
class WordLengthCount(override val uid: String) extends UnaryTransformer[Seq[Seq[String]], Array[String], WordLengthCount] {

  def this() = this(Identifiable.randomUID("word-length"))

  val maxWordLength: IntParam = new IntParam(this, "max", "maximum word length >=2", ParamValidators.gtEq(2))
  val minWordLength: IntParam = new IntParam(this, "min", "minimum word length >=1", ParamValidators.gtEq(1))

  def setMinWordLength(value: Int): this.type = set(minWordLength, value)

  def setMaxWordLength(value: Int): this.type = set(maxWordLength, value)

  def getMaxWordLength: Int = $(maxWordLength)

  def getMinWordLength: Int = $(minWordLength)

  override protected def createTransformFunc: (Seq[Seq[String]]) => Array[String] = {
    wordLength(_)
  }

  def wordLength(sentences: Seq[Seq[String]]): Array[String] = {
    var main = Map[Int,Int]()

    sentences.foreach(sentence => {
      val lengthMap = sentence.foldLeft(Map.empty[Int, Int]) {
        (map, word) => {
          if (word.length >= $(minWordLength) && word.length <= $(maxWordLength)) {
            map + (word.length -> (map.getOrElse(word.length, 0) + 1))
          }
          else {
            map
          }
        }
      }

      main = main ++ lengthMap.map {
        case (k, v) => k -> (v + main.getOrElse(k, 0))
      }

    })

    var vector = Vector.fill[String]($(maxWordLength)+1)("0")

    main.foreach {
      case (k, v) => vector = vector.updated(k, v.toString)
    }

    vector.toArray
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(ArrayType(StringType))), s"Input type must be Array[Array[String]] but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}
