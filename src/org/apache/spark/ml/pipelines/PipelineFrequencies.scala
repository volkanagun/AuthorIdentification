package org.apache.spark.ml.pipelines

import options.{ParamsFeatures, ParamsWeighting}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.pipes.{LoaderPipe, PipeVectorizer}

/**
  * Created by wolf on 04.07.2016.
  */
class PipelineFrequencies {


  def pipeline(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    if (ParamsFeatures.bowTerm) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.termsCol)
        .setOutputCol(PipelineWeighting.termFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.midLessDocFrequency)
    }

    if (ParamsFeatures.tokenStem) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenStemCol)
        .setOutputCol(PipelineWeighting.stemFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.midLessDocFrequency)
    }

    if (ParamsFeatures.tokenGram) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenGramCol)
        .setOutputCol(PipelineWeighting.tokenGramFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.midMoreDocFrequency)
    }



    if (ParamsFeatures.repeatedPuncs) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.repeatedPuncsCol)
        .setOutputCol(PipelineWeighting.repeatedPuncFreqCol)
        .setVocabSize(ParamsWeighting.midLowTF)
        .setMinDF(ParamsWeighting.minDocFrequency)
    }

    if (ParamsFeatures.textCharNGrams) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.textCharNGramsCol)
        .setOutputCol(PipelineWeighting.textCharGramFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.midMoreDocFrequency)
    }

    if (ParamsFeatures.phraseType) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.phraselsCol)
        .setOutputCol(PipelineWeighting.phraselTypeFreqCol)
        .setVocabSize(ParamsWeighting.midLowTF)
        .setMinDF(ParamsWeighting.minDocFrequency)
    }

    if (ParamsFeatures.functionWordPosTag) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.functionWordsPosTagsCol)
        .setOutputCol(PipelineWeighting.functionWordPosFreqCol)
        .setVocabSize(ParamsWeighting.midLowTF)
        .setMinDF(ParamsWeighting.minDocFrequency)
    }


    if (ParamsFeatures.tokenShape) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenShapeCol)
        .setOutputCol(PipelineWeighting.tokenShapeFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
    }

    if(ParamsFeatures.tokenEmoticon){
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenEmoticonCol)
        .setOutputCol(PipelineWeighting.tokenEmoticonFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
    }

    if(ParamsFeatures.tokenSuffix){
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenSuffixCol)
        .setOutputCol(PipelineWeighting.tokenSuffixFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
    }


    if(ParamsFeatures.tokenSym){
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.tokenSymbolCol)
        .setOutputCol(PipelineWeighting.tokenSymbolFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
    }



    if (ParamsFeatures.textShape) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.textShapeCol)
        .setOutputCol(PipelineWeighting.textShapeFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
    }

    if (ParamsFeatures.posTag) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(LoaderPipe.possesCol)
        .setOutputCol(PipelineWeighting.posTagFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.midLessDocFrequency)
    }

    if (ParamsFeatures.posTagGram) {
      stages = stages :+ new PipeVectorizer()
        .setInputCol(PipelineFeatures.posGramCol)
        .setOutputCol(PipelineWeighting.posGramFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.softDocFrequency)
    }

    if(ParamsFeatures.morphTag){
      stages = stages :+ new PipeVectorizer()
        .setInputCol(LoaderPipe.morphsCol)
        .setOutputCol(PipelineWeighting.morphTagFreqCol)
        .setVocabSize(ParamsWeighting.maxTF)
        .setMinDF(ParamsWeighting.minDocFrequency)
    }


    stages
  }
}
