package org.apache.spark.ml.classification

import options.ParamsClassification
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param._
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.param.shared.{HasMaxIter, HasThreshold, HasTol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
  * Created by wolf on 30.04.2016.
  */
class SVMKernelClassifier(override val uid: String) extends Classifier[Vector, SVMKernelClassifier, SVMKernelModel] with HasMaxIter with HasTol with HasThreshold{

  val kernelParam = new Param[KernelFunction](uid, "kernel-function", "SVM Kernel Function")
  val ppackSize = new IntParam(uid, "ppack size (r)", "ppack size (r)", (x: Int) => {
    x >= 1
  })


  //Single Model
  //Kernelized SVM
  def this() = this("svm-kernel-classifier")

  @Since("1.6.1")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  @Since("1.6.1")
  def setTol(value: Double): this.type = set(tol, value)

  def setKernelFunction(function: KernelFunction): this.type = set(kernelParam, function)

  def setMiniBatchSize(value: Int): this.type = set(ppackSize, value)

  def setThreshold(value:Double):this.type  = set(threshold,value)

  def getKernelFunction(): KernelFunction = $(kernelParam)

  def getPPackSize(): Int = $(ppackSize)




  setDefault(kernelParam -> new RBFKernel(ParamsClassification.rbfGamma), tol -> 0.001, maxIter -> 1000, threshold-> 0.0)

  override def copy(extra: ParamMap): SVMKernelClassifier = defaultCopy(extra)

  override protected def train(dataset: DataFrame): SVMKernelModel = {
    val niter = $(maxIter)
    val kernelFunction = $(kernelParam)
    val lambda = $(tol)
    val ppackr = $(ppackSize)

    val lpData = extractLabeledPoints(dataset)
      .map(lp=>LabeledPoint(if(lp.label==1.0) 1.0 else -1.0, lp.features))

    val model = SVMPPackSGD.train(kernelFunction, lpData, lambda, ppackr, niter)

    new SVMKernelModel(model)
  }


}


class SVMKernelModel(override val uid: String, val svmModel: SVMNonLinearDualModel) extends ClassificationModel[Vector, SVMKernelModel]
  with Serializable with ClassifierParams
  with MLWritable with MLReadable[SVMKernelModel] {

  private case class Data(numClasses: Int,
                          kernelFunction: KernelFunction,
                          threshold: Double, s: Double,
                          parameters: Array[(LabeledPoint, Double)])

  def this(svmModel: SVMNonLinearDualModel) = this(Identifiable.randomUID("svm-kernel-model"), svmModel)

  override protected def predict(features: Vector): Double = {
    svmModel.predict(features)
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = {
    val model = this
    new MLWriter with Logging {

      @org.apache.spark.annotation.Since("1.6.0") override protected
      def saveImpl(path: String): Unit = {
        DefaultParamsWriter.saveMetadata(model, path, sc)
        val data = new Data(numClasses,
          svmModel.kernel,
          svmModel.threshold.get,
          svmModel.s,
          svmModel.parameters)
        val dataPath = new Path(path, "data").toString
        sqlContext.createDataFrame(Seq[Data](data)).repartition(1).write.parquet(dataPath)
      }
    }
  }


  @org.apache.spark.annotation.Since("1.6.0")
  override def read: MLReader[SVMKernelModel] = {

    new MLReader[SVMKernelModel] {
      private val className = classOf[SVMKernelModel].getName

      @org.apache.spark.annotation.Since("1.6.0")
      override def load(path: String): SVMKernelModel = {
        val metadata = DefaultParamsReader.loadMetadata(path, sc)
        val dataPath = new Path(path, "data").toString
        val data = sqlContext.read.format("parquet").load(dataPath)
          .select("numClasses", "kernelFunction", "threshold", "s", "parameters").head()

        val numClasses = data.getInt(0)
        val kernelFunction = data.getAs[KernelFunction](1)
        val threshold = data.getDouble(2)
        val s = data.getDouble(3)
        val parameters = data.getAs[Array[(LabeledPoint, Double)]](4)

        val nonLinearDualModel = new SVMNonLinearDualModel(kernelFunction, s, parameters)
        val svmKernelModel = new SVMKernelModel(metadata.uid, nonLinearDualModel)
        DefaultParamsReader.getAndSetParams(svmKernelModel, metadata)
        svmKernelModel
      }
    }
  }

  override def copy(extra: ParamMap): SVMKernelModel = defaultCopy(extra)

  override def numClasses: Int = 2

  override protected def predictRaw(features: Vector): Vector = {
    val m = svmModel.predictPoint(features)
    Vectors.dense(-m, m)
  }
}


class SVMClassifier(override val uid: String) extends ProbabilisticClassifier[Vector, SVMClassifier, SVMClassifierModel] with HasMaxIter with HasTol {

  val miniBatchFraction = new DoubleParam(uid, "mini batch size", "mini batch size", (x: Double) => {
    x > 0.0
  })

  val regParam = new DoubleParam(uid, "regression parameter", "regParam", (x: Double) => x >= 0.0)


  val threshold = new DoubleParam(uid, "threshold", "threshold parameter", (x: Double) => x >= 0.0)

  val svmAlg = new SVMWithSGD()
    .setIntercept(true)

  //Single Model
  //Kernelized SVM
  def this() = this("svm-linear-classifier")

  @Since("1.6.1")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  @Since("1.6.1")
  def setTol(value: Double): this.type = set(tol, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setMiniBatchSize(value: Double): this.type = set(miniBatchFraction, value)

  def setThreshold(value: Double): this.type = set(threshold, value)

  def getMiniBatchSize(): Double = $(miniBatchFraction)

  def getRegParam(): Double = $(regParam)


  setDefault(tol -> 0.001, maxIter -> 1000, threshold -> 0.0)

  override def copy(extra: ParamMap): SVMClassifier = defaultCopy(extra)

  override protected def train(dataset: DataFrame): SVMClassifierModel = {
    val niter = $(maxIter)
    val lambda = $(tol)
    val miniBatch = $(miniBatchFraction)
    val regParameter = $(regParam)
    val thresholdParam = $(threshold)

    svmAlg.optimizer
      .setUpdater(new L1Updater)
      .setNumIterations(niter)
      .setMiniBatchFraction(miniBatch)
      .setConvergenceTol(lambda)
      .setRegParam(regParameter)
      .setStepSize(0.1)

    val lpData = extractLabeledPoints(dataset)
    val model = svmAlg.run(lpData)

    new SVMClassifierModel(uid, model.weights, model.intercept)
      .setThreshold(thresholdParam)
  }


}

class SVMClassifierModel(override val uid: String, val weights: Vector, val intercept: Double)
  extends  ProbabilisticClassificationModel[Vector, SVMClassifierModel]
  with Serializable with ClassifierParams
  with MLWritable with MLReadable[SVMClassifierModel] {

  override def numClasses: Int = 2

  private var threshold: Option[Double] = Some(0.0)

  /**
    * Sets the threshold that separates positive predictions from negative predictions. An example
    * with prediction score greater than or equal to this threshold is identified as an positive,
    * and negative otherwise. The default value is 0.0.
    */
  @Since("1.0.0")
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  def this(weights: Vector, intercept: Double) = this(Identifiable.randomUID("svm-linear-model"), weights, intercept)

  override def predict(dataMatrix: Vector) = {
    val margin = weights.toBreeze.dot(dataMatrix.toBreeze) + intercept
    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin
    }
  }

  override def predictRaw(dataMatrix: Vector): Vector = {
    val m = weights.toBreeze.dot(dataMatrix.toBreeze) + intercept
    Vectors.dense(-m, m)
  }


  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = rawPrediction

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = {
    new MLWriter {
      @org.apache.spark.annotation.Since("1.6.0") override protected
      def saveImpl(path: String): Unit = {

      }
    }
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def read: MLReader[SVMClassifierModel] = {
    new MLReader[SVMClassifierModel] {
      @org.apache.spark.annotation.Since("1.6.0")
      override def load(path: String): SVMClassifierModel = {
        val savedModel = SVMModel.load(sc, path)
        new SVMClassifierModel(uid, savedModel.weights, savedModel.intercept)

      }
    }
  }

  override def copy(extra: ParamMap): SVMClassifierModel = defaultCopy(extra)
}

@Since("0.8.0")
class SVMLinearModel @Since("1.1.0")(@Since("1.4.0") override val uid: String,
                                     @Since("1.0.0") val weights: Vector,
                                     @Since("0.8.0") val intercept: Double)
  extends ClassificationModel[Vector, SVMLinearModel] {

  private var threshold: Option[Double] = Some(0.0)

  /**
    * Sets the threshold that separates positive predictions from negative predictions. An example
    * with prediction score greater than or equal to this threshold is identified as an positive,
    * and negative otherwise. The default value is 0.0.
    */
  @Since("1.0.0")
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
    * Returns the threshold (if any) used for converting raw prediction scores into 0/1 predictions.
    */
  @Since("1.3.0")
  def getThreshold: Option[Double] = threshold

  /**
    * Clears the threshold so that `predict` will output raw prediction scores.
    */
  @Since("1.0.0")
  def clearThreshold(): this.type = {
    threshold = None
    this
  }


  override def predictRaw(dataMatrix: Vector): Vector = {
    val m = weights.toBreeze.dot(dataMatrix.toBreeze) + intercept
    Vectors.dense(-m, m)
  }


  override def numClasses: Int = 2

  override def copy(extra: ParamMap): SVMLinearModel = defaultCopy(extra)

  override def toString: String = {
    s"${super.toString}, numClasses = 2, threshold = ${threshold.getOrElse("None")}"
  }
}

