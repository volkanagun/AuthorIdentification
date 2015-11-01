package org.apache.spark.ml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}

import scala.util.matching.Regex

/**
 * Created by wolf on 31.10.2015.
 */
class WordForms(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("english-spell"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val capitals = functions.udf {
      sentences: Seq[Seq[String]] => {
        var vector = Vector.fill[Int](10)(0)
        sentences.foreach(sentence => {
          //lowercase word start
          if (sentence.head.matches("[a-zçöşğüı]\\p{L}+")) {
            vector = vector.updated(0, vector(0) + 1);
          }

          //word start
          if (sentence.head.matches("\\p{L}+")) {
            vector = vector.updated(1, vector(1) + 1);
          }

          //digit start
          if (sentence.head.matches("\\d+")) {
            vector = vector.updated(2, vector(2) + 1);
          }

          //NamedEntity start
          if (sentence.head.equals("NamedEntity")) {
            vector = vector.updated(3, vector(3) + 1);
          }

          //Greetings informal start
          if (sentence.head.toLowerCase.matches("(hello|hi|whats|wassup|hey|howdy|yo)")) {
            vector = vector.updated(4, vector(4) + 1);
          }

          //Greeting formal start
          if (sentence.head.toLowerCase.matches("(good|hi|hello|goodbye|greetings|dear|how|it's|nice)")) {
            vector = vector.updated(5, vector(5) + 1);
          }

          //Capitalized
          if (sentence.forall(word => word.matches("\\p{Lu}+"))) {
            vector = vector.updated(6, vector(6) + 1);
          }

          //Upper case start, lower camelcase
          sentence.foreach(word => {

            if (word.matches("\\p{Lu}\\p{L}+")) {
              vector = vector.updated(7, vector(7) + 1);
            }

            if (word.matches("[a-zçöşğüı]+\\p{Lu}")) {
              vector = vector.updated(8, vector(8) + 1);
            }

          })

        })

        vector
      }
    }

    dataset.select(col("*"), capitals(col($(inputCol))).as($(outputCol), metadata))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType, true))),
      s"Input type must be Array[Array[String]] but got $inputType.")
    val outputFields = schema.fields :+ StructField($(outputCol), ArrayType(StringType, false), schema($(inputCol)).nullable)
    StructType(outputFields)
  }


}
