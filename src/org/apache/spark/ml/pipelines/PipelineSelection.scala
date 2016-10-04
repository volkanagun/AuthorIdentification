package org.apache.spark.ml.pipelines

import options.{ParamsFeatures, ParamsSelection, ParamsWeighting}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.ml.pipes.ColumnRenamer

/**
  * Created by wolf on 29.07.2016.
  */
class PipelineSelection {

  //after tf-idf

  def pipeline():Array[PipelineStage]={

    var stages = Array[PipelineStage]()

    if(ParamsFeatures.bowTerm && ParamsWeighting.termFeatureWeighting  && ParamsSelection.termSelection){
      stages = stages ++ selection(PipelineWeighting.termWeightCol, PipelineSelection.termTransformedCol)
    }

    if(ParamsFeatures.tokenStem && ParamsWeighting.stemFeatureWeighting  &&ParamsSelection.stemSelection){
      stages = stages ++ selection(PipelineWeighting.stemWeightCol, PipelineSelection.stemTransformedCol)
    }


    if(ParamsFeatures.tokenGram && ParamsWeighting.tokenGramFeatureWeighting  &&ParamsSelection.tokenGramSelection){
      stages = stages ++ selection(PipelineWeighting.tokenGramWeightCol, PipelineSelection.tokenGramTransformedCol)
    }


    if(ParamsFeatures.textCharNGrams && ParamsWeighting.textCharGramFeatureWeighting  && ParamsSelection.textGramSelection){
      stages = stages ++ selection(PipelineWeighting.textCharGramWeightCol, PipelineSelection.textGramTransformedCol)
    }

    if(ParamsFeatures.tokenSuffix && ParamsWeighting.tokenSuffixFeatureWeighting  && ParamsSelection.tokenSuffixSelection){
      stages = stages ++ selection(PipelineWeighting.tokenSuffixWeightCol, PipelineSelection.tokenSuffixTransformedCol)
    }


    stages = stages
    stages
  }

  def selection(inputCol:String, outputCol:String):Array[PipelineStage]={
    var array = Array[PipelineStage]()

    if(ParamsSelection.pcaSelection) {
      array = array :+ new PCA().setInputCol(inputCol)
        .setOutputCol(outputCol)
        .setK(ParamsSelection.pcaK)

      array = array :+ new ColumnRenamer(true)
        .setInputCol(outputCol)
        .setOutputCol(inputCol)
    }

    array
  }


}

object PipelineSelection{
  val transformedFeaturesCol = "transformed-features"
  val termTransformedCol = "term-trans-features"
  val stemTransformedCol = "stem-trans-features"
  val tokenGramTransformedCol = "tokengram-trans-features"
  val textGramTransformedCol = "textgram-trans-features"
  val tokenSuffixTransformedCol = "tokensuffix-trans-features"


}
