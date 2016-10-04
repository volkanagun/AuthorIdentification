package org.apache.spark.ml.pipes

import java.io.PrintWriter

import data.dataset.DatasetStats
import options.ParamsUnique
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Created by wolf on 30.06.2016.
  */
class PipesStatistics(override val uid: String) extends Estimator[PipeStatModel] {
  //the parameters should be sink to file
  //like in the evalution as well as training stats should be calculated
  //like label -> count, total count for both training and test dataset
  def this() = this(Identifiable.randomUID("statistics-sink"))

  override def fit(dataset: DataFrame): PipeStatModel = {
    val trainingXML = DatasetStats.createAsXML(dataset)
    new PipeStatModel(uid,trainingXML)
  }

  override def copy(extra: ParamMap): Estimator[PipeStatModel] = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema


}

class PipeStatModel(override val uid: String, trainXML:String) extends Model[PipeStatModel]{



  override def copy(extra: ParamMap): PipeStatModel = {
    val copied = new PipeStatModel(uid, trainXML)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val testXML = DatasetStats.createAsXML(dataset)
    val filename = ParamsUnique.processedStatsFilename("dataset")

    new PrintWriter(filename){
      write("<ROOT>\n")
      write("<TRAINING>\n")
      write(trainXML)
      write("</TRAINING>\n")

      write("<TESTING>\n")
      write(testXML)
      write("</TESTING>\n")

      write("</ROOT>\n")
      close()

    }

    dataset

  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???


}
