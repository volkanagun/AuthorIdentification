package org.apache.spark.ml.feature.extraction

import opennlp.models.PosTaggerML
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}

/**
 * Created by wolf on 05.11.2015.
 */
class OpenPOSTagger(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("pos-tags"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val postagger = new PosTaggerML()

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata

    val posser = functions.udf {
      var posses: Seq[String] = Seq()
      sentences: Seq[Seq[String]] => {
        sentences.foreach(tokens=>{
          val tags = postagger.fit(tokens.toArray)
          posses = posses ++ tags

        })

        posses
      }
    }

    dataset.select(col("*"),posser(col($(inputCol))).as($(outputCol),metadata))

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType))), s"Input type must be Array[Array[String]] but got $inputType.")
    val outputFields = schema.fields :+ StructField($(outputCol), ArrayType(StringType, false), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
