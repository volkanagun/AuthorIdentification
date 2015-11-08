package org.apache.spark.ml.feature

import com.fasterxml.jackson.module.scala.modifiers.SeqTypeModifier
import opennlp.models.TokenizerML
import opennlp.tools.tokenize.TokenizerME
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{StructField, ArrayType, StringType, StructType}

/**
 * Created by wolf on 28.10.2015.
 */
class OpenTokenizer(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  val tokenizer = new TokenizerML()

  def this() = this(Identifiable.randomUID("tokenizer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val tokenize = functions.udf {
      sentences: Seq[String] => {
        var sentenceTokens = Vector[Vector[String]]()

        sentences.foreach(sentence => {
          val tokens = tokenizer.fit(sentence)
          sentenceTokens = sentenceTokens :+ tokens
        })

        sentenceTokens
      }
    }

    dataset.select(col("*"), tokenize(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): OpenTokenizer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {

    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(ArrayType(StringType)), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
