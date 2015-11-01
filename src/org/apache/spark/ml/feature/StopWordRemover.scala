package org.apache.spark.ml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}
import org.apache.spark.sql.functions._
import scala.io.Source

/**
 * Created by wolf on 31.10.2015.
 */
class StopWordRemover(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  val filename = "resources/dictionary/stopwords/stopwords.txt"
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

    val stops = functions.udf {
      sentences: Seq[Seq[String]] => {
        var flatWords = Seq[Seq[String]]()
        sentences.foreach(words => {
          val seq: Seq[String] = words.filter(word => !stopwords.contains(word))
          flatWords = flatWords :+ seq

        })

        flatWords
      }
    }


    dataset.select(col("*"), stops(col($(inputCol))).as($(outputCol), metadata))

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

