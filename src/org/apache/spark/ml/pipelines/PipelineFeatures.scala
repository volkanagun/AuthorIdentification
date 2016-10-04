package org.apache.spark.ml.pipelines

import java.io.PrintWriter

import options.ParamsFeatures
import org.apache.spark.ml.pipes._
import org.apache.spark.ml.{Context, PipelineStage}

/**
  * Created by wolf on 26.04.2016.
  */
class PipelineFeatures {

  val features = new Context()
  val textLengthRateFunc = features.textLengthRates()
  val textLengthRateMeta = features.textLengthRatesMeta()

  val textCharGramFunc = features.textCharGramFeatures()
  val textShapeFunc = features.textShapeFeatures()

  val tokenShapeFunc = features.sentenceTokenShapeFeatures()
  val tokenShelfFunc = features.sentenceTokenSelfFeatures()
  val tokenEmoticonFunc = features.sentenceTokenEmoticonFeatures()
  val tokenSuffixFunc = features.sentenceTokenSuffixPrefixFeatures()
  val tokenSymbolFunc = features.sentenceTokenSymbolFeatures()


  val tokenStemFunc = features.sentenceStemFeatures()
  val tokenGramFunc = features.sentenceTokenGramFeatures()
  val posGramFunc = features.sentencePOSGramFeatures()
  val morphRatioFunc = features.morphTagRatios()
  val morphRatioMeta = features.morphTagRatioMeta()

  val repeatedPuncsFunc = features.textRepeatedPuncFeatures()
  val phraselsFunc = features.turkishPhrasels()
  val functionWordPosTagsFunc = features.turkishFunctionWordPosTags()
  val functionWordPosTagCountsFunc = features.turkishFunctionWordPosCounts()

  val functionWordPosTagCountsMeta = features.turkishFunctionWordPosCountsMeta()
  val functionWordCountsFunc = features.turkishFunctionWordCounts()
  val functionWordCountsFuncMeta = features.turkishFunctionWordCountsMeta()

  val functionWordRatesFunc = features.functionWordRates()
  val functionWordRatesMeta = features.functionWordRatesMeta()

  val vocabRichTokens = features.vocabularyRichness()
  val vocabRichTokensMeta = features.vocabularyRichnessMeta("tokens")

  val positionalLMFunc = features.positionalLMScore()
  val positionalLMMeta = features.positionalLMScoreMeta()

  def explain(): this.type ={
    val printWriter = new PrintWriter("feature-explain.txt")
    features.explain(printWriter)
    printWriter.close()
    this
  }

  def pipeline() : Array[PipelineStage] = {
    var stages = Array[PipelineStage]()

    if(ParamsFeatures.lengthFeatureRates){
      stages = stages:+ new ParagraphRateExtractor(textLengthRateFunc, textLengthRateMeta)
        .setInputColumns(Array(LoaderPipe.paragraphsCol,LoaderPipe.sentencesCol,LoaderPipe.tokensCol))
        .setOutputColumn(PipelineFeatures.lengthRatesCol)
    }

    if(ParamsFeatures.tokenShape){
      stages = stages :+ new SentenceFeatureExtractor(tokenShapeFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenShapeCol)
    }

    if(ParamsFeatures.bowTerm){
      stages = stages :+ new SentenceFeatureExtractor(tokenShelfFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.termsCol)
    }

    if(ParamsFeatures.tokenStem){
      stages = stages :+ new SentenceFeatureExtractor(tokenStemFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenStemCol)
    }

    if(ParamsFeatures.tokenEmoticon){
      stages = stages :+ new SentenceFeatureExtractor(tokenEmoticonFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenEmoticonCol)
    }
    if(ParamsFeatures.tokenSuffix){
      stages = stages :+ new SentenceFeatureExtractor(tokenSuffixFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenSuffixCol)
    }

    if(ParamsFeatures.tokenSym){
      stages = stages :+ new SentenceFeatureExtractor(tokenSymbolFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenSymbolCol)
    }

    if(ParamsFeatures.tokenGram){
      stages = stages :+ new SentenceFeatureExtractor(tokenGramFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.tokenGramCol)
    }

    if(ParamsFeatures.posTagGram){
      stages = stages :+ new SentenceFeatureExtractor(posGramFunc)
        .setInputColumn(LoaderPipe.possesCol)
        .setOutputColumn(PipelineFeatures.posGramCol)
    }

    if(ParamsFeatures.repeatedPuncs){
      stages = stages :+ new TextFeatureExtractor(repeatedPuncsFunc)
        .setInputColumn(LoaderPipe.textCol)
        .setOutputColumn(PipelineFeatures.repeatedPuncsCol)
    }

    if(ParamsFeatures.textCharNGrams){
      stages = stages :+ new TextFeatureExtractor(textCharGramFunc)
        .setInputColumn(LoaderPipe.textCol)
        .setOutputColumn(PipelineFeatures.textCharNGramsCol)
    }

    if(ParamsFeatures.phraseType){
      stages = stages :+ new TextFeatureExtractor(phraselsFunc)
        .setInputColumn(LoaderPipe.textCol)
        .setOutputColumn(PipelineFeatures.phraselsCol)
    }

    if(ParamsFeatures.functionWordPosTag){
      stages = stages :+ new SentenceFeatureExtractor(functionWordPosTagsFunc)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.functionWordsPosTagsCol)

      stages = stages :+ new TextRateFeatureExtractor(functionWordPosTagCountsFunc, functionWordPosTagCountsMeta)
        .setInputColumn(LoaderPipe.sentencesCol)
        .setOutputColumn(PipelineFeatures.functionWordsPosCountsCol)
    }

    if(ParamsFeatures.functionWordRates){
      stages = stages :+ new SentenceRateExtractor(functionWordRatesFunc, functionWordRatesMeta)
        .setInputColumns(Array(LoaderPipe.sentencesCol,LoaderPipe.tokensCol))
        .setOutputColumn(PipelineFeatures.functionWordRatesCol)

      stages = stages :+ new TextRateFeatureExtractor("functionword-counts",functionWordCountsFunc, functionWordCountsFuncMeta)
        .setInputColumn(LoaderPipe.sentencesCol)
        .setOutputColumn(PipelineFeatures.functionWordsCountCol)
    }

    if(ParamsFeatures.textShape){
      stages = stages :+ new TextFeatureExtractor(textShapeFunc).setInputColumn(LoaderPipe.textCol)
        .setOutputColumn(PipelineFeatures.textShapeCol)
    }

    if(ParamsFeatures.vocabularyRichnessTokens){
      stages = stages :+ new TextRateFeatureExtractor("vocabulary-richness",vocabRichTokens,vocabRichTokensMeta)
        .setInputColumn(LoaderPipe.tokensCol)
        .setOutputColumn(PipelineFeatures.vocabRichTokensCol)
    }

    if(ParamsFeatures.lmRegularModel){
      stages = stages :+ new TextRateFeatureExtractor("lmRegularModel",positionalLMFunc,positionalLMMeta)
        .setInputColumn(LoaderPipe.sentencesCol)
        .setOutputColumn(PipelineFeatures.posRegularLMCol)
    }

    //TODO: Complex features
    /*if(ParamsFeatures.morphTag){
      stages = stages :+ new SetRateFeatureExtractor("morphTagRatios",morphRatioFunc, morphRatioMeta)
        .setInputColumn(LoaderPipe.morphSeqCol).setOutputColumn(PipelineFeatures.morphRatioCol)
    }*/

    if(ParamsFeatures.termHistogramRates){
      throw new IllegalArgumentException("Term histogram Rates Must be implemented...")
    }

    if(ParamsFeatures.functionWordHistogramRates){
      throw new IllegalArgumentException("Function word histogram rates must be implemented...")
    }

    stages
  }

}

object PipelineFeatures{
  val lengthRatesCol = "length-features"
  val termHistogramRatesCol = "termhistogram-features"
  val functionWordHistogramRatesCol = "functionhistogram-features"

  val tokenCol = "token-features"
  val tokenShapeCol = "token-shapes-features"

  val termsCol = "terms-features"
  val tokenStemCol = "tokenstem-features"
  val tokenSuffixCol = "tokensuffix-features"
  val tokenEmoticonCol = "tokenemoticon-features"
  val tokenSymbolCol = "tokensymbol-features"

  val tokenGramCol = "tokengram-features"

  val posGramCol = "posgram-features"
  val morphGramCol = "morphgram-features"
  val morphRatioCol = "morphratio-features"

  val repeatedPuncsCol = "repeatedpuncs-features"
  val textCharNGramsCol = "textchargram-features"
  val textShapeCol = "textshape-features"
  val vocabRichTokensCol = "vocabrichtokens-features"
  val posRegularLMCol = "posregularlm-features"


  val phraselsCol = "phrasels-features"
  val functionWordsPosTagsCol = "functionword-postag-features"
  val functionWordsPosCountsCol = "functionword-poscount-features"
  /*val functionWordsCol = "functionwords-features"*/
  val functionWordsCountCol = "functionwords-count-features"
  val functionWordRatesCol = "functionwordrates-features"

}
