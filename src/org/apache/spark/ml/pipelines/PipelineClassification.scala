package org.apache.spark.ml.pipelines


import options.{ParamsClassification, ParamsFeatures, ParamsUnique, ParamsWeighting}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification._
import org.apache.spark.ml.pipes._
import org.apache.spark.mllib.classification.{DotKernel, RBFKernel}

/**
  * Created by wolf on 30.04.2016.
  */
class PipelineClassification() extends Serializable {

  def pipeline(): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    val weightCols = new PipelineAssembler().weightCols()
    val classBased = isClassBased()
    if (ParamsClassification.createWekaDataset) {

      val fullARFF = ParamsUnique.wekaARFFFilename()
      val trainARFF = ParamsUnique.wekaARFFTrainFilename()
      val testARFF = ParamsUnique.wekaARFFTestFilename()

      val trainStats = ParamsUnique.wekaStatsTrainFilename()
      val testStats = ParamsUnique.wekaStatsTestFilename()

      val trainSample = ParamsUnique.wekaSampleTrainFilename()
      val testSample = ParamsUnique.wekaSampleTestFilename()

      val wekaSink = new WekaSink()
        .setInputCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorCol)
        .setOutputCol(PipelineClassification.featuresText)
        .setTrainFilename(trainARFF)
        .setTestFilename(testARFF)
        .setFullFilename(fullARFF)

      val wekaSample = new WekaSample().setTrainStats(trainStats)
        .setTestStats(testStats)
        .setTrainSample(trainSample)
        .setTestSample(testSample)

      stages = stages :+ wekaSink :+ wekaSample

    }
    else if (ParamsClassification.createLibSVMDataset) {
      stages = stages :+ new LibSVMSink()
        .setInputCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setOutputCol(PipelineClassification.featuresText)
    }

    else if (ParamsClassification.naiveBayesAuthor) {
      stages = stages :+ new NaiveBayes()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setSmoothing(1.0)

    }
    else if (ParamsClassification.logisticAuthor) {

      if (ParamsWeighting.tfidf && !ParamsClassification.classBasedAssemble) {

        stages = stages :+ new PipesLogistic()
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setMaxIter(ParamsClassification.logisticMaxIter)
          .setTol(ParamsClassification.logisticTol)

      }
      else {

        val classifier = new LogisticRegression()
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setMaxIter(ParamsClassification.logisticMaxIter)
          .setTol(ParamsClassification.logisticTol)

        val ovo = new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(true)
          .setWeightedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)

        stages = stages :+ ovo


      }


    }
    else if (ParamsClassification.dtTreeAuthor) {
      stages = stages :+ new DecisionTreeClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setRawPredictionCol(PipelineClassification.authorRawPrediction)
        .setMaxBins(ParamsClassification.maxBins)
        .setMaxDepth(ParamsClassification.maxDepth)
    }
    else if (ParamsClassification.rndForestAuthor) {
      stages = stages :+ new RandomForestClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setRawPredictionCol(PipelineClassification.authorRawPrediction)
        .setMaxBins(ParamsClassification.maxBins)
        .setMaxDepth(ParamsClassification.maxDepth)
        .setNumTrees(ParamsClassification.rndNumTrees)
    }
    else if (ParamsClassification.mpAuthor) {
      stages = stages :+ new MultilayerPerceptronClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setLayers(ParamsClassification.layerSize)
        .setTol(ParamsClassification.mpTol)
        .setMaxIter(ParamsClassification.mpMaxIter)

    }
    else if (ParamsClassification.svmAuthor) {

      val classifier = new SVMClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setMaxIter(ParamsClassification.svmMaxIter)
        .setTol(ParamsClassification.svmTol)
        .setMiniBatchSize(ParamsClassification.svmMiniBatchFraction)
        .setRegParam(ParamsClassification.svmRegParameter)
        .setThreshold(0.0)


      val ovr = if (ParamsWeighting.tfidf || !ParamsClassification.classBasedAssemble) {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(false)
          .setWeightedCols(weightCols)
      }
      else {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(true)
          .setWeightedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)

       /* new WeightedClassifier()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBasedCol(classBased)
          .setWeigtedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)*/
      }

      stages = stages :+ ovr

    }
    else if (ParamsClassification.svmRBFAuthor) {

      val rbfKernel = new RBFKernel(ParamsClassification.rbfGamma)
      val classifier = new SVMKernelClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setKernelFunction(rbfKernel)
        .setMaxIter(ParamsClassification.svmMaxIter)
        .setMiniBatchSize(ParamsClassification.svmPPackSize)
        .setTol(ParamsClassification.svmTol)

      val ovr = if (ParamsWeighting.tfidf || !ParamsClassification.classBasedAssemble) {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(false)
          .setWeightedCols(weightCols)
      }
      else {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(true)
          .setWeightedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)


      }

      stages = stages :+ ovr
    }
    else if (ParamsClassification.svmDotAuthor) {

      val dotKernel = new DotKernel()
      val classifier = new SVMKernelClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)
        .setKernelFunction(dotKernel)
        .setMaxIter(ParamsClassification.svmMaxIter)
        .setMiniBatchSize(ParamsClassification.svmPPackSize)
        .setTol(ParamsClassification.svmTol)

      val ovr = if (ParamsWeighting.tfidf) {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBased(classBased)
          .setWeightedCols(weightCols)
      }
      else {
        new WeightedClassifier()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBasedCol(classBased)
          .setWeigtedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)
      }

      stages = stages :+ ovr
    }
    else if (ParamsClassification.svmTransductiveAuthor) {
      val kernelType = "linear"
      val classifier = new SVMTransductiveClassifier()
        .setFeaturesCol(PipelineAssembler.featuresCol)
        .setLabelCol(PipelineLabelling.authorLabelCol)
        .setPredictionCol(PipelineClassification.authorPrediction)


      val ovr = if (ParamsWeighting.tfidf) {
        new OneVsOne()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
      }
      else {
        new WeightedClassifier()
          .setClassifier(classifier)
          .setFeaturesCol(PipelineAssembler.featuresCol)
          .setLabelCol(PipelineLabelling.authorLabelCol)
          .setPredictionCol(PipelineClassification.authorPrediction)
          .setClassBasedCol(classBased)
          .setWeigtedCols(weightCols)
          .setStaticCol(PipelineAssembler.featureStaticCol)
      }
      stages = stages :+ ovr
    }


    stages
  }


  def isClassBased(): Boolean = {
    if (ParamsWeighting.tfidf) false
    else true
  }
}

object PipelineClassification {
  val authorPrediction = "author-prediction"
  val authorRawPrediction = "author-raw"
  val featuresText = "features-text"


  val svmSMOAuthor = "svmSMOAuthor"
  val svmDotAuthor = "svmDotAuthor"
  val svmRBFAuthor = "svmRBFAuthor"
  val svmAuthor = "svmAuthor"
  val wekaSinkModel = "wekaSinkModel"
  val wekaStatsModel = "wekaStatsModel"

  val logisticAuthor = "logisticAuthor"
  val naiveBayesAuthor = "naiveBayesAuthor"
  val createLibSVMDataset = "createLibSVMDataset"

}
