package org.apache.spark.ml.pipelines

import options.{ParamsClassification, ParamsFeatures, ParamsWeighting}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.pipes._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}


/**
  * Created by wolf on 28.04.2016.
  */
class PipelineWeighting {
  //HashingTF, feature selection from spark methods
  //Custom selection and weighting methods
  //Use named index a hashmap to reference each stage index

  def idfSchema(): IDFWeightingSchema = {
    if (ParamsWeighting.tfidf) new TFIDF().setClassBased(ParamsClassification.classBasedAssemble)
    else if (ParamsWeighting.chiSquare) new ChiSquare()
    else if (ParamsWeighting.corrCoefficient) new CorrCoefficient()
    else if (ParamsWeighting.oddsRatio) new OddsRatio()
    else if (ParamsWeighting.probLiu) new ProbLIU()
    else if (ParamsWeighting.infoGain) new InfoGain()
    else new TFIDF()
  }



  def pipeline(): Array[PipelineStage] = {

    var stages = Array[PipelineStage]()


    val schema = idfSchema()

    if (ParamsFeatures.bowTerm && ParamsWeighting.termFeatureWeighting) {


      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setOutputCol(PipelineWeighting.termWeightCol)
        .setInputCol(PipelineWeighting.termFreqCol)

      if (ParamsWeighting.normalization) {

        stages = stages ++ normalizing(PipelineWeighting.termWeightCol,
          PipelineWeighting.termWeightNormCol)

      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.termFreqCol)

    }

    if (ParamsFeatures.tokenStem && ParamsWeighting.stemFeatureWeighting) {
      stages = stages :+ new IDFWeighter()
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.stemFreqCol)
        .setOutputCol(PipelineWeighting.stemWeightCol)
        .setWeightingModel(schema)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.stemWeightCol, PipelineWeighting.stemWeightNormCol)
      }


      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.stemFreqCol)
    }

    if (ParamsFeatures.tokenGram && ParamsWeighting.tokenGramFeatureWeighting) {
      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.tokenGramFreqCol)
        .setOutputCol(PipelineWeighting.tokenGramWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.tokenGramWeightCol, PipelineWeighting.tokenGramWeightNormCol)

      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.tokenGramFreqCol)


    }

    if (ParamsFeatures.posTagGram && ParamsWeighting.posGramFeatureWeighting) {

      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.posGramFreqCol)
        .setOutputCol(PipelineWeighting.posGramWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.posGramWeightCol, PipelineWeighting.posGramWeightNormCol)
      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.posGramFreqCol)

    }

    if (ParamsFeatures.repeatedPuncs && ParamsWeighting.repeatedPuncFeatureWeighting) {

      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.repeatedPuncFreqCol)
        .setOutputCol(PipelineWeighting.repeatedPuncWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.repeatedPuncWeightCol,PipelineWeighting.repeatedPuncNormCol)
      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.repeatedPuncFreqCol)

    }


    if (ParamsFeatures.textCharNGrams && ParamsWeighting.textCharGramFeatureWeighting) {

      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.textCharGramFreqCol)
        .setOutputCol(PipelineWeighting.textCharGramWeightCol)


      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.textCharGramWeightCol,PipelineWeighting.textCharGramNormCol)
      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.textCharGramFreqCol)
    }

    if (ParamsFeatures.phraseType && ParamsWeighting.phraselTypeFeatureWeighting) {

      stages = stages :+ new IDFWeighter()
        .setWeightingModel(schema)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setInputCol(PipelineWeighting.phraselTypeFreqCol)
        .setOutputCol(PipelineWeighting.phraselTypeWeightCol)


      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.phraselTypeWeightCol,PipelineWeighting.phraselTypeNormCol)
      }

      stages = stages :+ new ColumnRemover()
        .setInputCol(PipelineWeighting.phraselTypeFreqCol)
    }


    if (ParamsFeatures.functionWordPosTag && ParamsWeighting.functionPOSFeatureWeighting) {

      stages = stages ++ weighting(schema, PipelineLabelling.authorLabelCol, PipelineWeighting.functionWordPosFreqCol,
        PipelineWeighting.functionWordPosWeightCol)


      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.functionWordPosWeightCol,PipelineWeighting.functionWordPosNormCol)
      }

    }


    if (ParamsFeatures.tokenShape && ParamsWeighting.tokenShapeFeatureWeighting) {
      stages = stages ++ weighting(schema, PipelineLabelling.authorLabelCol, PipelineWeighting.tokenShapeFreqCol,
        PipelineWeighting.tokenShapeWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.tokenShapeWeightCol,PipelineWeighting.tokenShapeWeightNormCol)
      }


    }

    if (ParamsFeatures.textShape && ParamsWeighting.textShapeFeatureWeighting) {

      stages = stages ++ weighting(schema, PipelineLabelling.authorLabelCol,
        PipelineWeighting.textShapeFreqCol,
        PipelineWeighting.textShapeWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.textShapeWeightCol,PipelineWeighting.textShapeWeightNormCol)
      }

    }


    if (ParamsFeatures.posTag && ParamsWeighting.posTagFeatureWeighting) {

      stages = stages ++ weighting(schema, PipelineLabelling.authorLabelCol,
        PipelineWeighting.posTagFreqCol,
        PipelineWeighting.posTagWeightCol)

      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.posTagWeightCol,PipelineWeighting.posTagNormCol)
      }
    }

    if (ParamsFeatures.morphTag && ParamsWeighting.morphTagFeatureWeighting) {

      stages = stages ++ weighting(schema, PipelineLabelling.authorLabelCol, PipelineWeighting.morphTagFreqCol,
        PipelineWeighting.morphTagWeightCol)


      if(ParamsWeighting.normalization){
        stages = stages ++ normalizing(PipelineWeighting.morphTagWeightCol,PipelineWeighting.morphTagNormCol)
      }

    }

    stages = stages ++ renaming()

    stages
  }

  protected def normalizing(inputCol:String, outputCol:String): Array[PipelineStage] ={
    var stages = Array[PipelineStage]()
    stages = stages :+ new Normalizer()
      .setP(ParamsWeighting.normLevel)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)

    stages = stages :+ new ColumnRenamer(true)
      .setInputCol(outputCol)
      .setOutputCol(inputCol)

    stages
  }

  protected def weighting(schema:IDFWeightingSchema,
                          labelCol:String,
                          inputCol:String,
                          outputCol:String):Array[PipelineStage]={

    var stages = Array[PipelineStage]()
    stages = stages :+ new IDFWeighter()
      .setWeightingModel(schema)
      .setLabelCol(labelCol)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)

    stages = stages :+ new ColumnRemover()
      .setInputCol(inputCol)

    stages
  }


  protected def renaming(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.termFreqCol)
      .setOutputCol(PipelineWeighting.termWeightCol)

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.stemFreqCol)
      .setOutputCol(PipelineWeighting.stemWeightCol)

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.tokenShapeFreqCol)
      .setOutputCol(PipelineWeighting.tokenShapeWeightCol)

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.tokenSymbolFreqCol)
      .setOutputCol(PipelineWeighting.tokenSymbolWeightCol)

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.tokenEmoticonFreqCol)
      .setOutputCol(PipelineWeighting.tokenEmoticonWeightCol)


    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.tokenSuffixFreqCol)
      .setOutputCol(PipelineWeighting.tokenSuffixWeightCol)


    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.textShapeFreqCol)
      .setOutputCol(PipelineWeighting.textShapeWeightCol)


    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.posTagFreqCol)
      .setOutputCol(PipelineWeighting.posTagWeightCol)

    stages = stages :+ new ColumnRenamer()
      .setInputCol(PipelineWeighting.morphTagFreqCol)
      .setOutputCol(PipelineWeighting.morphTagWeightCol)

    stages
  }

}

object PipelineWeighting {
  val termFreqCol = "term-freq"
  val stemFreqCol = "stem-freq"
  val tokenGramFreqCol = "tokengram-freq"
  val tokenShapeFreqCol = "token-shape-freq"
  val tokenSymbolFreqCol = "token-symbol-freq"
  val tokenEmoticonFreqCol = "token-emoticon-freq"
  val tokenSuffixFreqCol = "token-suffix-freq"

  val posTagFreqCol = "postag-freq"
  val posTagWeightCol = "postag-weights"
  val posTagNormCol = "postag-norms"

  val morphTagFreqCol = "morphtag-freq"
  val morphTagWeightCol = "morphtag-weights"
  val morphTagNormCol = "morphtag-norms"

  val posGramFreqCol = "posgram-freq"
  val repeatedPuncFreqCol = "repeatedpunc-freq"
  val textShapeFreqCol = "text-shape-freq"
  val textCharGramFreqCol = "textchargram-freq"
  val phraselTypeFreqCol = "phraseltype-freq"
  val functionWordPosFreqCol = "functionwords-pos-freq"

  val termWeightCol = "term-weights"
  val termWeightNormCol = "term-weights-norm"

  val tokenGramWeightCol = "tokengram-weights"
  val tokenGramWeightNormCol = "tokengram-weights-norm"

  val stemWeightCol = "stem-weights"
  val stemWeightNormCol = "stem-weights-norm"

  val tokenShapeWeightCol = "token-shape-weights"
  val tokenShapeWeightNormCol = "token-shape-weights-norm"

  val tokenSymbolWeightCol = "token-symbol-weights"
  val tokenSymbolWeightNormCol = "token-symbol-weights-norm"

  val tokenEmoticonWeightCol = "token-emoticon-weights"
  val tokenEmoticonWeightNormCol = "token-emoticon-weights-norm"

  val tokenSuffixWeightCol = "token-suffix-weights"
  val tokenSuffixWeightNormCol = "token-suffix-weights-norm"

  val textShapeWeightCol = "text-shape-weights"
  val textShapeWeightNormCol = "text-shape-weights-norm"

  val posGramWeightCol = "posgram-weights"
  val posGramWeightNormCol = "posgram-weights-norm"

  val repeatedPuncWeightCol = "repeatedpunc-weights"
  val repeatedPuncNormCol = "repeatedpunc-weights-norm"


  val textCharGramWeightCol = "textchargram-weights"
  val textCharGramNormCol = "textchargram-norms"

  val phraselTypeWeightCol = "phraseltype-weights"
  val phraselTypeNormCol = "phraseltype-norms"

  val functionWordPosWeightCol = "functionword-pos-weights"
  val functionWordPosNormCol = "functionword-pos-norms"
}
