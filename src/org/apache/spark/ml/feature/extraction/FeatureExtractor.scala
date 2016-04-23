package org.apache.spark.ml.feature.extraction

import org.apache.commons.lang.math.RandomUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntArrayParam, Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.util.IntParam

/**
  * Created by wolf on 17.04.2016.
  */



class SentenceFeatureExtractor(override val uid:String, val func:Seq[String]=>Seq[String]) extends Transformer with HasInputCol with HasOutputCol
{
  //Takes tokens and extracts features

  def this(func:Seq[String]=>Seq[String]) = this(Identifiable.randomUID("sentence-features"), func)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      tokens: Seq[String] => {
        func(tokens)
      }
    }
    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol),metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType,true)),
      s"Input type must be Array[StringType] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}


class TextFeatureExtractor(override val uid:String, val func:String=>Seq[String]) extends Transformer with HasInputCol with HasOutputCol
{
  //Takes tokens and extracts features

  def this(func:String=>Seq[String]) = this(Identifiable.randomUID("sentence-features"), func)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      sentence: String => {
        func(sentence)
      }
    }
    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol),metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType,true)),
      s"Input type must be Array[StringType] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
