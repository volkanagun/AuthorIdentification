package org.apache.spark.ml.pipes


import language.util.TextSlicer
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, functions, types}
import org.apache.spark.sql.types.{ArrayType, StringType, _}


/**
  * Created by wolf on 14.09.2016.
  */
class PipesTextSlicer(override val uid:String) extends Transformer with HasInputCol with HasOutputCol {

  def this()=this(Identifiable.randomUID("textslicer"))

  val sliceNum = new IntParam(this, "slice-num","number of slices")

  def setSliceNum(value:Int):this.type =set(sliceNum, value)

  /**
    * Apply to tokens column
    * @param value
    * @return
    */
  def setInputCol(value:String):this.type =set(inputCol, value)

  /**
    * Output column is array of tokens
    * @param value
    * @return
    */
  def setOutputCol(value:String):this.type =set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val slicer = functions.udf {
      sentences: Seq[String] => {
        TextSlicer.chunkRegular(sentences, $(sliceNum))
      }
    }

    val schema = transformSchema(dataset.schema)
    val outputSchema = schema($(outputCol))
    dataset.withColumn($(outputCol),slicer(functions.col($(inputCol))),outputSchema.metadata)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val typeCandidates = List(ArrayType(types.StringType, true), ArrayType(types.StringType, false))
    SchemaUtils.checkColumnTypes(schema,$(inputCol),typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), ArrayType(ArrayType(types.StringType, true)))
  }
}
