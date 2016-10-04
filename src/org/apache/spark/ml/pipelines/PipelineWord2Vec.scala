package org.apache.spark.ml.pipelines

import options.{ParamsClassification, ParamsFeatures, ParamsWord2Vec}
import options.Resources.{DocumentResources, Word2VecResources}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.pipes.{LoaderPipe, PipesWord2Vec, PipesWord2VecModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 20.09.2016.
  */
class PipelineWord2Vec extends Serializable {

  def pipeline(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()

    if(ParamsFeatures.useWord2Vec) {
      stages = stages :+ new PipesWord2Vec()
        .setVectorSize(ParamsWord2Vec.vectorWV2Size)
        .setMaxIter(ParamsWord2Vec.maxW2VIteration)
        .setMinTrainingCount(ParamsWord2Vec.minW2VTrainingCount)
        .setModelFilename(Word2VecResources.word2VecDir)
        .setWinSize(ParamsWord2Vec.winW2VSize)
        .setInputCol(LoaderPipe.sentenceTokensCol)
        .setMergingType(ParamsFeatures.word2VecType)
        .setOutputCol(PipelineWord2Vec.word2VecOutCol)
        .setBuildModel(ParamsClassification.createWord2Vec)
    }

    stages
  }

  def fit(df: DataFrame): Unit = {
    new PipesWord2Vec()
      .setVectorSize(ParamsWord2Vec.vectorWV2Size)
      .setMaxIter(ParamsWord2Vec.maxW2VIteration)
      .setMinTrainingCount(ParamsWord2Vec.minW2VTrainingCount)
      .setModelFilename(Word2VecResources.word2VecDir)
      .setWinSize(ParamsWord2Vec.winW2VSize)
      .setInputCol(LoaderPipe.tokensCol)
      .setBuildModel(ParamsClassification.createWord2Vec)
      .setMergingType(ParamsFeatures.word2VecType)
      .setOutputCol(PipelineWord2Vec.word2VecOutCol)
      .fit(df)

  }
}

object PipelineWord2Vec {
  val word2VecOutCol = "word2vec-out"
}
