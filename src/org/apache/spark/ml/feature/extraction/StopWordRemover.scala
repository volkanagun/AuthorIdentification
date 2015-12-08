package org.apache.spark.ml.feature.extraction

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}

import scala.io.Source

/**
 * Created by wolf on 31.10.2015.
 */
class StopWordRemover(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  val filename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/dictionary/stopwords/stopwords.txt"
  lazy val stopwords = load

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("stopwords"))


  def load: List[String] = {
    Source.fromFile(filename).mkString.split("\n").toList
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata

    val stopremove = functions.udf {
      sentences: Seq[Seq[String]] => {
        var flatWords = Seq[Seq[String]]()
        sentences.foreach(words => {
          val seq: Seq[String] = words.filter(word => !stopwords.contains(word))
          flatWords = flatWords :+ seq
        })
        flatWords
      }
    }


    dataset.select(col("*"), stopremove(col($(inputCol))).as($(outputCol), metadata))

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType, false))), s"Input type must be Array[Array[String]] but got $inputType.")
    val outputFields = schema.fields :+ StructField($(outputCol), ArrayType(StringType, false), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}

