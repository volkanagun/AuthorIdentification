package org.apache.spark.ml.pipes


import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasLabelCol}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by wolf on 12.06.2016.
  */
class PipesSplitter(override val uid:String) extends Transformer with HasInputCol with HasLabelCol {

  override def transform(dataset: DataFrame): DataFrame = {
    val sqlContext = dataset.sqlContext
    val sentencesColumn = $(inputCol)
    val labelColumn = $(labelCol)
    val df = dataset.select(sentencesColumn, labelColumn).rdd.flatMap{
      case Row(sentences:Seq[Any], author:String) => {
        val rows = sentences.map(sentence=>{ Row(sentence, author)} )
        rows
      }
    }
    val schema = StructType(
      Array(
        StructField(sentencesColumn, StringType),
        StructField(labelColumn, StringType)))
    sqlContext.createDataFrame(df, schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}
