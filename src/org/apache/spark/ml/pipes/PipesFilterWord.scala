package org.apache.spark.ml.pipes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
  * Created by wolf on 13.09.2016.
  */
class PipesFilterWordWithPOS(override val uid:String) extends Transformer with HasInputCols with HasOutputCol{

  def this() = this(Identifiable.randomUID("tokenFilterByPOS"))

  val posTags = new StringArrayParam(this,"postag-filter","patterns for pos tag to be filtered")

  /**
    * First column is words, second pos tags
    * @param value
    * @return
    */
  def setInputCols(value:Array[String]):this.type=set(inputCols, value)
  def setPOSTags(value:Array[String]):this.type=set(posTags,value)
  def setOutputCol(value:String):this.type =set(outputCol, value)

  setDefault(inputCols->Array(LoaderPipe.tokensCol, LoaderPipe.possesCol), posTags->Array("NN", "NNS","NNP"))

  override def transform(dataset: DataFrame): DataFrame = {
    val inputs = $(inputCols)
    val matches = $(posTags)
    val posTagColumn = inputs(0)
    val tokenColumn = inputs(1)

    val filter = functions.udf {
      (posses:Seq[String], tokens: Seq[String]) => {
        val posLength = posses.length
        val tokenLength = tokens.length
        var tokenSeq = Seq[String]()
        if(posLength==tokenLength) {
          for (i <- 0 until posLength) {
            val pos = posses(i)
            val token = tokens(i)
            if(matches.exists(matching=>pos.matches(matching))){
              tokenSeq = tokenSeq :+ token
            }
          }
        }

        tokenSeq
      }
    }

    val schema = transformSchema(dataset.schema)
    val outputSchema = schema($(outputCol))
    dataset.select(functions.col("*"),
      filter(functions.col(posTagColumn), functions.col(tokenColumn)).as($(outputCol),
      outputSchema.metadata))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    //first validate then transform
    $(inputCols).map(c => {
      val inputType = schema(c).dataType
      require(inputType.sameType(ArrayType(StringType, true)),
        s"Input column $c type must be Array[StringType] but got $inputType.")
    })

    val outputFields = schema.fields :+
      StructField($(outputCol),ArrayType(StringType, false))

    StructType(outputFields)
  }
}
