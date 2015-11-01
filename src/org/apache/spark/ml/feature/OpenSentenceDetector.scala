package org.apache.spark.ml.feature



import scala.collection.{mutable, JavaConversions}
import models.opennlp.SentenceDetectorML
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}

/**
 * Created by wolf on 16.09.2015.
 */
@Experimental
class OpenSentenceDetector(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("sentenceDetector"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  val sentenceDetectorML = new SentenceDetectorML()

  override def transform(dataset: DataFrame): DataFrame = {

    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val split =
      functions.udf {
        text: String => {
          sentenceDetectorML.fit(text).toSeq
        }

      }

    dataset.select(col("*"), split(col($(inputCol))).as($(outputCol), metadata))

  }

  override def copy(extra: ParamMap): OpenSentenceDetector = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(StringType),
      s"Input type must be StringType but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
