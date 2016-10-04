package org.apache.spark.ml.pipelines

import options.Resources.LDAResources
import options.{ParamsClassification, ParamsFeatures, ParamsLDAModel, ParamsWeighting}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.pipes._
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 13.09.2016.
  */
class PipelineTopicModels extends Serializable {

  def createLDAPipeline(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    val nounTags = ParamsLDAModel.topicPOSFilter

    //TODO: CHECK IF THERE IS A PROBLEM

    stages = stages :+ new PipesFilterWordWithPOS()
      .setInputCols(Array(LoaderPipe.possesCol, LoaderPipe.tokensCol))
      .setOutputCol(PipelineTopicModels.nounFilterCol)
      .setPOSTags(nounTags)

    stages = stages :+ new PipeVectorizer()
      .setMinDF(ParamsWeighting.minDocFrequency)
      .setInputCol(PipelineTopicModels.nounFilterCol)
      .setOutputCol(PipelineTopicModels.nounTopicCol)
      .setVocabSize(ParamsWeighting.maxTF)

    stages = stages :+ new SamplerLDA()
      .setModelFilename(LDAResources.ldaModelDir)
      .setVocabularyCol(PipelineTopicModels.nounTopicCol)
      .setProcessCol(PipelineTopicModels.nounTopicCol)
      .setOutputCol(PipelineTopicModels.topicVectorCol)
      .setCreateModel(ParamsClassification.createLDAmodel)
      .setSliceNum(ParamsLDAModel.topicSliceNum)

    stages

  }

  def pipeline():Array[PipelineStage]={
    var stages = Array[PipelineStage]()

    if(ParamsFeatures.topicModel){

        //Filtering nouns affects positioning
        stages = stages :+ new ColumnRenamer(false)
          .setInputCol(LoaderPipe.tokensCol)
          .setOutputCol(PipelineTopicModels.nounFilterCol)

      //Slice text
        stages = stages :+ new PipesTextSlicer()
          .setInputCol(PipelineTopicModels.nounFilterCol)
          .setOutputCol(PipelineTopicModels.sliceCol)
          .setSliceNum(ParamsLDAModel.topicSliceNum)

        stages = stages :+ new SamplerLDA()
          .setModelFilename(LDAResources.ldaModelDir)
          .setVocabularyCol(PipelineTopicModels.nounTopicCol)
          .setProcessCol(PipelineTopicModels.sliceCol)
          .setOutputCol(PipelineTopicModels.topicVectorCol)
          .setCreateModel(ParamsClassification.createLDAmodel)
          .setSliceNum(ParamsLDAModel.topicSliceNum)


    }

    stages
  }

  /*def pipeline(): Array[PipelineStage] = {

    var stages = Array[PipelineStage]()
    val nounTags = ParamsLDAModel.topicPOSFilter

    if (ParamsFeatures.topicModel) {

      if (ParamsClassification.createLDAmodel) {
        stages = stages :+ new PipesFilterWordWithPOS()
          .setInputCols(Array(LoaderPipe.possesCol, LoaderPipe.tokensCol))
          .setOutputCol(PipelineTopicModels.nounFilterCol)
          .setPOSTags(nounTags)
      }
      else {
        stages = stages :+ new ColumnRenamer(false)
          .setInputCol(LoaderPipe.tokensCol)
          .setOutputCol(PipelineTopicModels.nounFilterCol)
      }


      stages = stages :+ new PipeVectorizer()
        .setMinDF(ParamsWeighting.minDocFrequency)
        .setInputCol(PipelineTopicModels.nounFilterCol)
        .setOutputCol(PipelineTopicModels.nounTopicCol)
        .setVocabSize(ParamsWeighting.maxTF)



      if (ParamsFeatures.slicedTopicModel) {
        stages = stages :+ new PipesTextSlicer()
          .setInputCol(PipelineTopicModels.nounFilterCol)
          .setOutputCol(PipelineTopicModels.sliceCol)
          .setSliceNum(ParamsLDAModel.topicSliceNum)

        stages = stages :+ new SamplerLDA()
          .setModelFilename(LDAResources.ldaModelDir)
          .setVocabularyCol(PipelineTopicModels.nounTopicCol)
          .setProcessCol(PipelineTopicModels.sliceCol)
          .setOutputCol(PipelineTopicModels.nounTopicVectorCol)
          .setPositionalTopic(ParamsFeatures.slicedTopicModel)
          .setCreateModel(ParamsClassification.createLDAmodel)
          .setSliceNum(ParamsLDAModel.topicSliceNum)
      }
      else {
        stages = stages :+ new SamplerLDA()
          .setModelFilename(LDAResources.ldaModelDir)
          .setVocabularyCol(PipelineTopicModels.nounTopicCol)
          .setProcessCol(PipelineTopicModels.nounTopicCol)
          .setOutputCol(PipelineTopicModels.nounTopicVectorCol)
          .setPositionalTopic(ParamsFeatures.slicedTopicModel)
          .setCreateModel(ParamsClassification.createLDAmodel)
          .setSliceNum(ParamsLDAModel.topicSliceNum)
      }


    }

    stages
  }*/

  def ldaTransform(df: DataFrame): PipelineModel = {
    new Pipeline().setStages(createLDAPipeline()).fit(df)
  }
}

object PipelineTopicModels {
  val nounFilterCol = "noun-words"
  val nounTopicCol = "noun-topics"
  val topicVectorCol = "topic-vector"
  val sliceCol = "slice-col"
}
