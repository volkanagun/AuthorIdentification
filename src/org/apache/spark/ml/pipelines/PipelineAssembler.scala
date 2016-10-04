package org.apache.spark.ml.pipelines

import options.{ParamsClassification, ParamsFeatures, ParamsWeighting}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.VectorAssembler



/**
  * Created by wolf on 03.05.2016.
  */
class PipelineAssembler {

  def pipeline(): Array[PipelineStage]= {
    var stages = Array[PipelineStage]()
    val inputCols = numericColumns()

    if (ParamsWeighting.tfidf || !ParamsClassification.classBasedAssemble) {
      stages = stages :+ new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol(PipelineAssembler.featuresCol)
    }
    else {
      stages = stages :+ new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol(PipelineAssembler.featureStaticCol)

    }

    stages
  }

  def numericColumns(): Array[String] = {
    var cols = Array[String]()

    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Rate features">

    if (ParamsFeatures.functionWordRates) {
      cols = cols :+ PipelineFeatures.functionWordRatesCol :+ PipelineFeatures.functionWordsCountCol

    }

    if(ParamsFeatures.functionWordPosTag){
      cols = cols :+ PipelineFeatures.functionWordsPosCountsCol
    }

    if (ParamsFeatures.lengthFeatureRates) {
      cols = cols :+ PipelineFeatures.lengthRatesCol

    }

    if (ParamsFeatures.termHistogramRates) {
      cols = cols :+ PipelineFeatures.termHistogramRatesCol

    }

    if (ParamsFeatures.functionWordHistogramRates) {
      cols = cols :+ PipelineFeatures.functionWordHistogramRatesCol
    }

    if(ParamsFeatures.vocabularyRichnessTokens){
      cols = cols :+ PipelineFeatures.vocabRichTokensCol
    }

    if(ParamsFeatures.lmRegularModel){
      cols = cols :+ PipelineFeatures.posRegularLMCol
    }

    if(ParamsFeatures.topicModel){
      cols = cols :+ PipelineTopicModels.topicVectorCol
    }

    if(ParamsFeatures.useWord2Vec){
      cols = cols :+ PipelineWord2Vec.word2VecOutCol
    }

    if (!ParamsClassification.classBasedAssemble || ParamsWeighting.tfidf) {
      cols = cols ++ weightCols()
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////


    cols
  }

  def weightCols():Array[String]={
    var cols = Array[String]()
    if (ParamsFeatures.bowTerm) {
      cols = cols :+ PipelineWeighting.termWeightCol
    }

    if (ParamsFeatures.tokenShape) {
      cols = cols :+ PipelineWeighting.tokenShapeWeightCol
    }

    if (ParamsFeatures.tokenSym) {
      cols = cols :+ PipelineWeighting.tokenSymbolWeightCol
    }

    if (ParamsFeatures.tokenSuffix) {
      cols = cols :+ PipelineWeighting.tokenSuffixWeightCol
    }

    if (ParamsFeatures.tokenEmoticon) {
      cols = cols :+ PipelineWeighting.tokenEmoticonWeightCol
    }

    if (ParamsFeatures.tokenStem) {
      cols = cols :+ PipelineWeighting.stemWeightCol
    }

    if(ParamsFeatures.tokenGram){
      cols = cols :+ PipelineWeighting.tokenGramWeightCol
    }


    if(ParamsFeatures.textShape){
      cols = cols :+ PipelineWeighting.textShapeWeightCol
    }

    if(ParamsFeatures.repeatedPuncs){
      cols = cols :+ PipelineWeighting.repeatedPuncWeightCol
    }

    if(ParamsFeatures.functionWordPosTag){
      cols = cols :+ PipelineWeighting.functionWordPosWeightCol
    }

    if(ParamsFeatures.phraseType){
      cols = cols :+ PipelineWeighting.phraselTypeWeightCol
    }

    if(ParamsFeatures.textCharNGrams){
      cols = cols :+ PipelineWeighting.textCharGramWeightCol
    }

    if(ParamsFeatures.posTag){
      cols = cols :+ PipelineWeighting.posTagWeightCol
    }

    if(ParamsFeatures.morphTag){
      cols = cols :+ PipelineWeighting.morphTagWeightCol //:+ PipelineFeatures.morphRatioCol
    }

    cols
  }
}

object PipelineAssembler {
  val featuresCol = "features"
  val featureStaticCol = "features-static"

  val vectorAssembler = "vectorAssembler"
}
