package org.apache.spark.ml.pipelines

import options.ParamsClassification
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.pipes.{LoaderPipe, PipeIndexToString}
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 29.04.2016.
  */
class PipelineLabelling {
  //Label existing dataset to create different labels for different classification tasks
  //For instance label genres, or categorize sub text, and/or combine it with authors

  val indexerForward = new StringIndexer()
    .setInputCol(LoaderPipe.authorCol)
    .setOutputCol(PipelineLabelling.authorLabelCol)
    .setHandleInvalid("skip")

  val indexerBackward = new PipeIndexToString()
    .setInputCol(PipelineClassification.authorPrediction)
    .setOutputCol(PipelineLabelling.authorPredictedLabelCol)
    .setMetaCol(PipelineLabelling.authorLabelCol)

  def preClassification(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()

    stages = stages:+indexerForward

    stages
  }

  def postClassification(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    if(!(ParamsClassification.createWekaDataset || ParamsClassification.createLibSVMDataset))
    {
      stages = stages:+ indexerBackward
    }

    stages

  }


}

object PipelineLabelling {

  val authorLabelCol = "author-label"
  val authorCol = LoaderPipe.authorCol
  val authorPredictedLabelCol = "author-predicted-label"

}
