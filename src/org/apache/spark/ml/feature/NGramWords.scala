package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.param.{ParamMap, ParamValidators, IntParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}
import org.apache.spark.sql.{functions, DataFrame}

/**
 * Created by wolf on 31.10.2015.
 */
class NGramWords(override val uid:String) extends Transformer with HasInputCol with HasOutputCol{
  def this() = this(Identifiable.randomUID("chargram"))

  val min: IntParam = new IntParam(this, "min", "minimum number of ngrams >=1", ParamValidators.gtEq(1))
  val max: IntParam = new IntParam(this, "max", "minimum number of ngrams >=2", ParamValidators.gtEq(2))

  def setMin(value: Int): this.type = set(min, value)

  def setMax(value: Int): this.type = set(max, value)

  def getMax: Int = $(max)

  def getMin: Int = $(min)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(max, 8)
  setDefault(min, 2)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val ngrams = functions.udf{
      sentences:Seq[Seq[String]]=>{
        var wordgrams: Array[Array[String]] = Array()

        sentences.foreach(wordseq => {
          var senseq: Seq[String] = Seq();
          for (i <- $(min) to $(max)) {
            senseq = senseq ++ wordseq.iterator.sliding(i).withPartial(false).map(_.mkString(" "))
          }
          wordgrams = wordgrams :+ senseq.toArray
        })

        wordgrams
      }
    }

    dataset.select(col("*"), ngrams(col($(inputCol))).as($(outputCol),metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType,true))),
      s"Input type must be Array[Array[StringType]] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(ArrayType(StringType, true),true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): NGramWords = defaultCopy(extra)
}
