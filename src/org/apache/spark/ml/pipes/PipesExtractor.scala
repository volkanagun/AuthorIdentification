package org.apache.spark.ml.pipes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions}

/**
  * Created by wolf on 17.04.2016.
  */

class SentenceFeatureExtractor(override val uid: String, val func: Seq[String] => Seq[String]) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: Seq[String] => Seq[String]) = this(Identifiable.randomUID("sentence-features"), func)


  def setInputColumn(value: String) = set(inputCol, value)
  def setOutputColumn(value:String)=set(outputCol,value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      tokens: Seq[String] => {
        func(tokens)
      }
    }

    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType, true)),
      s"Input type must be Array[StringType] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }

}

/**
  *
  * @param uid
  * @param func sentences, and tokens of text are processed to double sequence
  */
class SentenceRateExtractor(override val uid: String, val func: (Seq[String], Seq[String]) => Seq[Double], val attrs:Array[Attribute])
  extends Transformer with HasInputCols with HasOutputCol  with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: (Seq[String], Seq[String]) => Seq[Double], attrs:Array[Attribute]) = this(Identifiable.randomUID("sentence-features"), func, attrs)


  def setInputColumns(value: Array[String]) = set(inputCols, value)
  def setOutputColumn(value:String)=set(outputCol,value)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val columns = $(inputCols).map(column => col(column))
    val metadata = outputSchema($(outputCol)).metadata
    val rater = functions.udf {
      (sentences: Seq[String], tokens: Seq[String]) => {
        val array = func(sentences, tokens).toArray
        Vectors.dense(array).toSparse
      }
    }
    dataset.select(col("*"), rater(columns(0), columns(1)).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    $(inputCols).map(c => {
      val inputType = schema(c).dataType
      require(inputType.sameType(ArrayType(StringType, true)),
        s"Input column $c type must be Array[StringType] but got $inputType.")
    })

    val outputFields = schema.fields :+
      StructField($(outputCol), new VectorUDT)

    StructType(outputFields)
  }
}

class ParagraphRateExtractor(override val uid: String, val func: (Seq[String], Seq[String], Seq[String]) => Seq[Double], val attrs:Array[Attribute]) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: (Seq[String], Seq[String], Seq[String]) => Seq[Double], attrs:Array[Attribute]) = this(Identifiable.randomUID("paragraph-features"), func, attrs)

  def setInputColumns(value: Array[String]) = set(inputCols, value)
  def setOutputColumn(value:String)=set(outputCol,value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val columns = $(inputCols).map(column => col(column))
    val metadata = outputSchema($(outputCol)).metadata
    val rater = functions.udf {
      (paragraphs: Seq[String], sentences: Seq[String], tokens: Seq[String]) => {
        val array = func(paragraphs, sentences, tokens).toArray
        Vectors.dense(array).toSparse
      }
    }
    dataset.select(col("*"), rater(columns(0), columns(1), columns(2)).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    $(inputCols).map(c => {
      val inputType = schema(c).dataType
      require(inputType.sameType(ArrayType(StringType, true)),
        s"Input column $c type must be Array[StringType] but got $inputType.")
    })

    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

class TextFeatureExtractor(override val uid: String, val func: String => Seq[String]) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: String => Seq[String]) = this(Identifiable.randomUID("sentence-features"), func)


  def setInputColumn(value: String) = set(inputCol, value)
  def setOutputColumn(value:String)=set(outputCol,value)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      sentence: String => {
        func(sentence)
      }
    }

    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType

    require(inputType.sameType(StringType),
      s"Input type must be Array[StringType] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}

class TextRateFeatureExtractor(override val uid: String, val func: Seq[String] => Seq[Double], val attrs:Array[Attribute]) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: Seq[String] => Seq[Double], attrs:Array[Attribute]) = this(Identifiable.randomUID("text-rate-features"), func, attrs)


  def setInputColumn(value: String):this.type = set(inputCol, value)
  def setOutputColumn(value:String):this.type = set(outputCol,value)


  override def transform(dataset: DataFrame): DataFrame = {
    println("Doing "+uid)
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      text: Seq[String] => {
        val arr = func(text).toArray
        Vectors.dense(arr).toSparse
      }
    }

    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
      val col = $(inputCol)
      val inputType = schema(col).dataType
      require(inputType.sameType(ArrayType(StringType, true)),
        s"Input column $col type must be Array[StringType] but got $inputType.")

    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

class SetRateFeatureExtractor(override val uid: String, val func: Seq[Seq[String]] => Seq[Double], val attrs:Array[Attribute]) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable{
  //Takes tokens and extracts features

  def this(func: Seq[Seq[String]] => Seq[Double], attrs:Array[Attribute]) = this(Identifiable.randomUID("text-rate-features"), func, attrs)


  def setInputColumn(value: String):this.type = set(inputCol, value)
  def setOutputColumn(value:String):this.type = set(outputCol,value)


  override def transform(dataset: DataFrame): DataFrame = {
    println("Doing "+uid)
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val extraction = functions.udf {
      set: Seq[Seq[String]] => {
        val arr = func(set).toArray
        Vectors.dense(arr).toSparse
      }
    }

    dataset.select(col("*"), extraction(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
      val col = $(inputCol)
      val inputType = schema(col).dataType
      require(inputType.sameType(ArrayType(ArrayType(StringType, false))),
        s"Input column $col type must be Array[Array[StringType]] but got $inputType.")

    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}