package org.apache.spark.ml.classification

import breeze.optimize.proximal.QuadraticMinimizer
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasMaxIter, HasSeed, HasTol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.util.Random

/** Params for SVM. */
private[ml] trait SVMParams extends PredictorParams with HasSeed with HasMaxIter with HasTol {
  /**
    * kernel type: linear, rbf, gaussian, customize
    * Default: linear
    *
    * @group param
    */
  final val kernelType: Param[String] = new Param[String](this, "kernelType",
    " svm kernel type",
    ParamValidators.inArray[String](Array("linear", "rbf", "gaussian")))

  /** @group getParam */
  final def getKernelType: String = $(kernelType)

  setDefault(tol -> 1e-4, maxIter -> 10, kernelType -> "linear")
}

class SVM (override val uid: String)
  extends Classifier[Vector, SVM, SVMSMOModel] with SVMParams with Serializable {

  def this() = this(Identifiable.randomUID("svm"))

  import org.apache.spark.ml.classification.SVM._

  def setKernelType(value: String): this.type = set(kernelType, value)

  /**
    * Set the maximum number of iterations.
    * Default is 100.
    *
    * @group setParam
    */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  override protected def train(dataset: DataFrame): SVMSMOModel = {


    //GARBAGE IMPLMENET YOUR OWN

    val lpData = extractLabeledPoints(dataset).collect()
    val model = train(lpData.toList)
    /*lpData.mapPartitions{ iter: Iterator[LabeledPoint] => Iterator(train(iter.toList))}.cache()*/
    if ($(kernelType) == "linear") {
      /*val w = Statistics.colStats(subModels.map(_.asInstanceOf[SVMDotModel].weight)).mean
      val b = subModels.map(_.asInstanceOf[SVMDotModel].b).mean()*/
      val linearModel = model.asInstanceOf[SVMDotModel]
      new SVMDotModel(uid, linearModel.weight, linearModel.b)
    }
    else {
      /*val alphaArray = subModels.map(_.asInstanceOf[SVMRbfModel].alpha).flatMap(a => a).collect()
      val dataArray = subModels.map(_.asInstanceOf[SVMRbfModel].supportingVectors)
        .flatMap(a => a).collect()
      val b = subModels.map(_.asInstanceOf[SVMRbfModel].b).mean()*/
      val kernelModel = model.asInstanceOf[SVMRbfModel]
      new SVMRbfModel(uid, kernelModel.alpha, kernelModel.supportingVectors, kernelModel.b)
    }
  }


  private def train(dataset: List[LabeledPoint]): SVMSMOModel = {
    val data =  dataset.map(p => LabeledPoint(if(p.label == 1.0) 1.0 else -1.0, p.features)).toArray

    val labels = data.map(_.label)
    val C = 0.5 // C value. Decrease for more regularization
    val tol =  1e-4 // numerical tolerance. Don't touch unless you're pro
    val numPasses = 10 // how many passes over data with no change before we halt? Increase for more precision.

    // instantiate kernel according to options. kernel can be given as string or as a custom function
    val kernel = if($(kernelType) == "linear") LinearKernel else RBFKernal

    val N = data.length
    val D = data(0).features.size
    val alpha = Array.fill[Double](N)(0)
    var b = 0.0

    var iter = 0
    var passes = 0
    def kernelResult = (i: Int, j: Int) => kernel(data(i).features, data(j).features)

    while(passes < numPasses && iter < $(maxIter)) {
      var alphaChanged = 0
      for (i <- 0 until N) {
        val Ei = marginOne(data(i).features, alpha, data, b) - labels(i)
        if ((labels(i) * Ei < -tol && alpha(i) < C)
          || (labels(i) * Ei > tol && alpha(i) > 0)) {

          // alpha_i needs updating! Pick a j to update it with
          var j = i
          while (j == i) j = new Random().nextInt(N)
          val Ej = marginOne(data(j).features, alpha, data, b) - labels(j)

          // calculate L and H bounds for j to ensure we're in [0 C]x[0 C] box
          val ai = alpha(i)
          val aj = alpha(j)
          var L = 0D
          var H = C
          if (labels(i) == labels(j)) {
            L = math.max(0, ai + aj - C)
            H = Math.min(C, ai + aj)
          } else {
            L = Math.max(0, aj - ai)
            H = Math.min(C, C + aj - ai)
          }

          if (Math.abs(L - H) < 1e-4) {}
          else {
            val kij = kernelResult(i, j)
            val kii = kernelResult(i, i)
            val kjj = kernelResult(j, j)

            val eta = 2 * kij - kii - kjj
            if (eta < 0) {
              var newaj = aj - labels(j) * (Ei - Ej) / eta
              if (newaj > H) newaj = H
              if (newaj < L) newaj = L
              if (Math.abs(aj - newaj) < 1e-9) {} else {
                alpha(j) = newaj
                val newai = ai + labels(i) * labels(j) * (aj - newaj)
                alpha(i) = newai

                // update the bias term
                val b1 = b - Ei - labels(i) * (newai - ai) * kii
                -labels(j) * (newaj - aj) * kij
                val b2 = b - Ej - labels(i) * (newai - ai) * kij
                -labels(j) * (newaj - aj) * kjj
                b = 0.5 * (b1 + b2)
                if (newai > 0 && newai < C) b = b1
                if (newaj > 0 && newaj < C) b = b2
                alphaChanged += 1
              }
            }
          }
        }
      }

      iter+= 1
      println(s"iter number $iter, alphaChanged = $alphaChanged, passes=$passes");
      if(alphaChanged == 0) passes+= 1
      else passes= 0
    }

    if($(kernelType) == "linear"){
      val w = new Array[Double](D)
      for(j <- 0 until D) {
        var s= 0.0
        for(i <- 0 until N) {
          s+= alpha(i) * labels(i) * data(i).features(j)
        }
        w(j) = s
      }
      new SVMDotModel(uid, Vectors.dense(w), b)
    }
    else{
      // okay, we need to retain all the support vectors in the training data,
      // we can't just get away with computing the weights and throwing it out

      // But! We only need to store the support vectors for evaluation of testing
      // instances. So filter here based on this.alpha[i]. The training data
      // for which this.alpha[i] = 0 is irrelevant for future.
      val supportingVectors = data.zip(alpha).filter(p => p._2 > 1e-7)
      new SVMRbfModel(uid, supportingVectors.map(_._2), supportingVectors.map(_._1), b)
    }
  }

  override def copy(extra: ParamMap): SVM = defaultCopy(extra)

}

object SVM extends Serializable {

  def LinearKernel = (v1: Vector, v2: Vector) => {
    var s = 0D
    for(q <- 0 until v1.size) { s += v1(q) * v2(q) }
    s
  }

  def RBFKernal = (v1: Vector, v2: Vector) => {
    val sigma = 0.5
    var s = 0D
    for(q <- 0 until v1.size) { s += (v1(q) - v2(q)) * (v1(q) - v2(q)) }
    Math.exp(- s / (2.0 * sigma * sigma))
  }

  def polyKernel = (v1: Vector, v2: Vector) => {

  }

  private[classification] def marginOne(inst: Vector, alpha: Array[Double], data: Array[LabeledPoint], b: Double): Double =  {
    data.zip(alpha).map{ case (point, alp) =>
      point.label * alp * LinearKernel(inst, point.features)
    }.sum + b
  }
}

abstract class SVMSMOModel extends ClassificationModel[Vector, SVMSMOModel] with Serializable {
  override def numClasses: Int = 2
}

class SVMDotModel private[ml](
                                   override val uid: String,
                                   val weight: Vector,
                                   val b: Double)
  extends SVMSMOModel with Serializable {

  override def predict(v: Vector): Double = {
    val value = v.toArray.zip(weight.toArray).map{ case (d1, d2) => d1 * d2 }.sum + b
    if(value > 0) 1 else 0
  }


  override protected def predictRaw(v: Vector): Vector = {
    val margin = v.toArray.zip(weight.toArray).map{ case (d1, d2) => d1 * d2 }.sum + b
    Vectors.dense(margin,-margin)
  }

  override def copy(extra: ParamMap): SVMSMOModel = {
    copyValues(new SVMDotModel(uid, weight, b), extra)
  }
}

class SVMRbfModel private[ml] (
                                override val uid: String,
                                val alpha: Array[Double],
                                val supportingVectors: Array[LabeledPoint],
                                val b: Double)
  extends SVMSMOModel with Serializable {

  override def predict(v: Vector): Double = {
    val value = SVM.marginOne(v, alpha, supportingVectors, b)
    if(value > 0) 1 else 0
  }


  override protected def predictRaw(v: Vector): Vector = {
    val value = SVM.marginOne(v, alpha, supportingVectors, b)
    Vectors.dense(value,-value)
  }

  override def copy(extra: ParamMap): SVMSMOModel = {
    copyValues(new SVMRbfModel(uid, alpha, supportingVectors, b), extra)
  }
}