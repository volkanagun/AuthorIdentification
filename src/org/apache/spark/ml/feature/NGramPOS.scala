package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, ParamValidators, IntParam}
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}
import org.apache.spark.sql.{functions, DataFrame}

/**
 * Created by wolf on 05.11.2015.
 */
class NGramPOS(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {
  def this() = this(Identifiable.randomUID("posgram"))

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

  setDefault(max, 3)
  setDefault(min, 2)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata
    val ngrams = functions.udf {
      posses: Seq[String] => {
        var posgrams: Array[String] = Array()

        for (i <- $(min) to $(max)) {
          posgrams = posgrams ++ posgrams.iterator.sliding(i).withPartial(false).map(_.mkString(" "))
        }

        posgrams
      }
    }

    dataset.select(col("*"), ngrams(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType, true)),
      s"Input type must be Array[StringType] but got $inputType.")
    val outputFields = schema.fields :+
      StructField($(outputCol), ArrayType(StringType, true), schema($(inputCol)).nullable)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): NGramWords = defaultCopy(extra)

}
