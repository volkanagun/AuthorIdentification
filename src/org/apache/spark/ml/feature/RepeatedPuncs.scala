package org.apache.spark.ml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}

import scala.util.matching.Regex

/**
 * Created by wolf on 29.10.2015.
 */
class RepeatedPuncs(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("repeating-punctuations"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: DataFrame): DataFrame = {
    //Find repeated words, and punctuations
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val puncpattern = new Regex("([\\p{Punct}\\@\\'\\,\\&])(\\1)+")
    val function =
      functions.udf {
        sentences: Seq[String] => {
          var puncs : Array[String] = Array()
          sentences.foreach(sentence => {
            val matches = puncpattern.findAllIn(sentence).toArray
            puncs = puncs.++(matches)
          })

          puncs

        }
      }

    dataset.select(col("*"), function(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): RepeatedPuncs = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType, true)), s"Input type must be Array[String] but got $inputType.")
    val outputFields = schema.fields :+ StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
