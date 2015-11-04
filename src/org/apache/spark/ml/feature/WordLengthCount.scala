package org.apache.spark.ml.feature


import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.{Transformer, UnaryTransformer}
import org.apache.spark.ml.param.{ParamMap, ParamValidators, IntParam}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

/**
 * Created by wolf on 30.10.2015.
 */
class WordLengthCount(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("word-length"))

  val maxWordLength: IntParam = new IntParam(this, "max", "maximum word length >=2", ParamValidators.gtEq(2))
  val minWordLength: IntParam = new IntParam(this, "min", "minimum word length >=1", ParamValidators.gtEq(1))

  def setMinWordLength(value: Int): this.type = set(minWordLength, value)

  def setMaxWordLength(value: Int): this.type = set(maxWordLength, value)

  def getMaxWordLength: Int = $(maxWordLength)

  def getMinWordLength: Int = $(minWordLength)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  setDefault(numFeatures -> (1 << 18))

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val wordlength = functions.udf {
      sentences: Seq[Seq[String]] => {
        wordgrams(sentences)
      }
    }

    dataset.select(col("*"), wordlength(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): WordLengthCount = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType, true))), s"Input type must be Array[StringType] but got $inputType.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  def wordgrams(sentences: Seq[Seq[String]]): org.apache.spark.mllib.linalg.Vector = {
    var main = Map[Int,Double]()

    sentences.foreach(sentence => {
      val lengthMap = sentence.foldLeft(Map.empty[Int, Double]) {
        (map, word) => {
          if (word.length >= $(minWordLength) && word.length <= $(maxWordLength)) {
            map + (word.length -> (map.getOrElse[Double](word.length, 0) + 1))
          }
          else {
            map
          }
        }
      }

      main = main ++ lengthMap.map {
        case (k, v) => k -> (v + main.getOrElse[Double](k, 0))
      }

    })

    var vector = Vector.fill[Double]($(maxWordLength)+1)(0)

    main.foreach {
      case (k, v) => vector = vector.updated(k, v)
    }

    Vectors.dense(vector.toArray)
  }


}
