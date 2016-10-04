package org.apache.spark.ml.pipes

import java.io.PrintWriter

import language.model.{LMBuilder, LanguageModel}
import options.Resources.{Global, LMResource}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by wolf on 05.08.2016.
  */
class PipesSamplerLM(override val uid:String) extends Transformer {

  def this() = this(Identifiable.randomUID("languageSampler"))

  val sampleCol = new Param[String](this,"sampleColumn","sample from this text column")
  val typeCol = new Param[String](this,"typeColumn","determines the type of the sample")

  def setSampleCol(value:String) = set(sampleCol, value)
  def setTypeCol(value:String) = set(typeCol, value)



  protected def aggregate(dataFrame: DataFrame): String = {

    //select all types by typeCol
    //collect as list
    //in foreach list do filter and aggregate
    //write it to file
    val customAggregate = new TextAggregate()
    val mainText = dataFrame.select($(sampleCol))
      .agg(customAggregate(dataFrame.col($(sampleCol))).as("main-text"))
      .head().getString(0)

    mainText
  }

  override def transform(dataset: DataFrame): DataFrame = {
    //select sample column and type column
    //apply sampling in aggregate function
    //aggregate to text
    println(dataset.count())
    val types = dataset.select($(typeCol))
      .distinct().collect().map(row=>row.getString(0))

    types.foreach(value=>{
      val filename = LMResource.modelsDir+ value+".bin"
      val textAggregate = new TextAggregate()
      val aggregation = dataset.filter(dataset($(typeCol))===value)
        .agg(textAggregate(dataset($(sampleCol)))).head().getString(0)

      new LMBuilder(3).create(aggregation, filename)

    })

    dataset

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }


}


object PipesTextLM{
  val ngram = 3
  //generate language models from sampled text in a directory
  
}