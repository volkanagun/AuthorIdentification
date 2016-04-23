package org.apache.spark.mllib.classification

import breeze.linalg.DenseVector
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{BLAS, Vectors, Vector}
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearModel}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 23.12.2015.
  */
class SVMNonLinearModel(kernel: KernelFunction, override val weights: Vector, override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable with Saveable with PMMLExportable {
  private var threshold: Option[Double] = Some(0.0)

  /**
    * :: Experimental ::
    * Sets the threshold that separates positive predictions from negative predictions. An example
    * with prediction score greater than or equal to this threshold is identified as an positive,
    * and negative otherwise. The default value is 0.0.
    */
  @Since("1.0.0")
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
    * :: Experimental ::
    * Returns the threshold (if any) used for converting raw prediction scores into 0/1 predictions.
    */
  @Since("1.3.0")
  @Experimental
  def getThreshold: Option[Double] = threshold

  /**
    * :: Experimental ::
    * Clears the threshold so that `predict` will output raw prediction scores.
    */
  @Since("1.0.0")
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }


  override protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector, intercept: Double): Double = {
    val margin = kernel.compute(weightMatrix, dataMatrix) + intercept
    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin
    }
  }

  override protected def formatVersion: String = "1.0.0"

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {}
}

class SVMNonLinearDualModel(kernel: KernelFunction, s: Double, model: RDD[(LabeledPoint, Double)])
  extends ClassificationModel with Serializable with Saveable with PMMLExportable {
  private var threshold: Option[Double] = Some(0.0)
  private val parameters = model.collect()

  /**
    * :: Experimental ::
    * Sets the threshold that separates positive predictions from negative predictions. An example
    * with prediction score greater than or equal to this threshold is identified as an positive,
    * and negative otherwise. The default value is 0.0.
    */
  @Since("1.0.0")
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
    * :: Experimental ::
    * Returns the threshold (if any) used for converting raw prediction scores into 0/1 predictions.
    */
  @Since("1.3.0")
  @Experimental
  def getThreshold: Option[Double] = threshold

  /**
    * :: Experimental ::
    * Clears the threshold so that `predict` will output raw prediction scores.
    */
  @Since("1.0.0")
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }


  def predictPoint(dataMatrix: Vector): Double = {


    var sum = 0d
    for (i <- 0 until parameters.length) {
      val (k, v) = parameters.apply(i)
      sum += v * k.label * kernel.compute(dataMatrix, k.features)
    }
    val margin = s * sum
    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin
    }
  }

  override protected def formatVersion: String = "1.0.0"

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {}

  @Since("1.0.0")
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.map(vector => {
      predict(vector)
    })
  }

  @Since("1.0.0")
  override def predict(testData: Vector): Double = {
    predictPoint(testData)
  }
}

@Since("0.8.0")
object SVMNonLinearWithSGD {

  /**
    * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using the specified step size. Each iteration uses
    * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
    * gradient descent are initialized using the initial weights provided.
    *
    * NOTE: Labels used in SVM should be {0, 1}.
    *
    * @param input RDD of (label, array of features) pairs.
    * @param numIterations Number of iterations of gradient descent to run.
    * @param stepSize Step size to be used for each iteration of gradient descent.
    * @param regParam Regularization parameter.
    * @param miniBatchFraction Fraction of data to be used per iteration.

    */
  @Since("0.8.0")
  def train( kernelFunction: KernelFunction,
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): SVMNonLinearModel = {

    //Train with inputs
    //Huge operations with aggregate functions
    val size = input.first().features.size
    val scaler = new StandardScaler(withStd = true, withMean = false).fit(input.map(_.features))
    val data = input.map(lp => (if (lp.label <= 0) -1 else 1, scaler.transform(lp.features))).cache()

    var r = stepSize
    val m = 0.05

    var w = Vectors.zeros(size)
    var b = -1.0
    val n = size

    val bcWeights = input.context.broadcast(w)

    for (iter <- 1 to numIterations) {

      val sample = data.sample(false, miniBatchFraction, 42 + iter).collect()

      for (j <- 0 until sample.length) {
        val pair = sample(j)
        val yi = pair._1
        val xi = pair._2
        if (yi * (kernelFunction.compute(xi, w) + b) < m) {
          val grad = kernelFunction.gradient(xi, w)
          val reg = kernelFunction.regularizer(w)
          BLAS.scal(r * yi, grad)
          BLAS.axpy(1, grad, w)
          b += r * yi
        }

        val arr = w.toArray
        val reg = kernelFunction.regularizerGradient(w)

        for (k <- 0 until arr.length) {
          val sgn = if (arr(k) > 0) 1 else if (arr(k) < 0) -1 else 0
          val update = sgn * Math.max(0, Math.abs(arr(k) - r * regParam * reg(k)))
          arr.update(k, update)
        }

        w = Vectors.dense(arr)
      }

      if (iter == 50) r = r / 10

      println("Iteration : " + iter)
    }

    new SVMNonLinearModel(kernelFunction, w, b)
  }

  def train(kernel: KernelFunction, training: RDD[LabeledPoint], numIterations: Int): SVMNonLinearModel = {
    SVMNonLinearWithSGD.train(kernel, training, numIterations, 0.05, 0.0001, 0.5)
  }


  @Since("0.8.0")
  def trainParallel(
                     kernelFunction: KernelFunction,
                     input: RDD[LabeledPoint],
                     numIterations: Int,
                     stepSize: Double,
                     regParam: Double,
                     miniBatchFraction: Double): SVMNonLinearModel = {

    //Train with inputs
    //Huge operations with aggregate functions
    val size = input.first().features.size
    val scaler = new StandardScaler(withStd = true, withMean = false).fit(input.map(_.features))
    val data = input.map(lp => (if (lp.label <= 0) -1 else 1, scaler.transform(lp.features))).cache()

    var r = stepSize
    val m = 0.05

    var w = Vectors.zeros(size)
    var b = -1.0
    val n = size

    for (iter <- 1 to numIterations) {

      val bcWeights = input.context.broadcast(w)

      val (gradientSum, interceptSum, miniBatchSize) = data.sample(false, miniBatchFraction, 42 + iter)
        .treeAggregate((DenseVector.zeros[Double](n), 0.0, 0L))(
          seqOp = (c, v) => {
            // c: (grad, intercept, count), v: (label, features)
            val pair = v
            val yi = pair._1
            val xi = pair._2
            //Calculate gradient, intercept, and count
            if (yi * (kernelFunction.compute(xi, bcWeights.value) + b) < m) {
              val grad = kernelFunction.gradient(xi, bcWeights.value)
              val intercept = r * yi
              BLAS.scal(r * yi, grad)
              val dense = new DenseVector[Double](grad.toArray)
              (dense, intercept, c._3 + 1)
            }
            else {
              (c._1, c._2, c._3 + 1)
            }

          },
          combOp = (c1, c2) => {
            // c: (grad, loss, count)
            (c1._1 += c2._1, c1._2 + c2._2, c1._3 + c2._3)
          });

      val average = 1d / miniBatchSize
      val sum: Vector = Vectors.dense(gradientSum.toArray)
      BLAS.axpy(average, sum, w)
      b += average * interceptSum

      val arr = w.toArray
      val reg = kernelFunction.regularizerGradient(w)

      for (k <- 0 until arr.length) {
        val sgn = if (arr(k) > 0) 1 else if (arr(k) < 0) -1 else 0
        val update = sgn * Math.max(0, Math.abs(arr(k) - r * regParam * reg(k)))
        arr.update(k, update)
      }

      w = Vectors.dense(arr)

      if (iter % 50 == 0) r = r / 2

      println("Iteration : " + iter)
    }

    new SVMNonLinearModel(kernelFunction, w, b)
  }

}

object SVMPPackSGD {


  def train(kernelFunction: KernelFunction,
            input: RDD[LabeledPoint],lambda: Double = 0.01,
            minibatchSize: Int,
            numIterations: Int): SVMNonLinearDualModel = {

    val sc = input.sparkContext
    var model = input.map(x => (x, 0D))
    val data = input
    var s = 1D

    //var working_data = IndexedRDD(data.zipWithUniqueId().map { case (k, v) => (v, (k, 0D)) })
    var indexed_data = data.zipWithUniqueId().map { case (k, v) => (v, (k, 0D)) }

    var norm = 0D
    var alpha = 0D
    var t = 1
    var i = 0
    var j = 0
    var updateCount = 0

    val pair_idx = sc.parallelize(Array.range(0, minibatchSize).flatMap(x => (Array.range(x, minibatchSize).map(y => (x, y)))))
    val broad_kernel_func = sc.broadcast(kernelFunction)

    while (t <= numIterations) {

      //val sample = working_data.takeSample(true, minibatchSize)
      val sample = indexed_data.takeSample(true, minibatchSize)
      val broad_sample = sc.broadcast(sample)
      val kernel = broad_kernel_func.value
      //val yp = broad_sample.value.map(x => (working_data.map { case (k, v) => (v._1.label * v._2 * kernel.compute(v._1.features, x._2._1.features)) }.reduce((a, b) => a + b)))
      val yp = broad_sample.value.map(x => (indexed_data.map { case (k, v) => (v._1.label * v._2 * kernel.compute(v._1.features, x._2._1.features)) }.reduce((a, b) => a + b)))
      val y = sample.map(x => x._2._1.label)
      var local_set = Map[Long, (LabeledPoint, Double)]()
      // Compute kernel inner product pairs
      val inner_prod = pair_idx.map(x => (x, kernel.compute(sample(x._1)._2._1.features, sample(x._2)._2._1.features))).collectAsMap()

      // Compute sub gradients
      for (i <- 0 until minibatchSize) {
        t = t + 1
        s = (1 - 1D / (t)) * s
        for (j <- (i + 1) until (minibatchSize)) {
          yp(j) = (1 - 1D / (t)) * yp(j)
        }
        if (y(i) * yp(i) < 1) {
          norm = norm + (2 * y(i)) / (lambda * t) * yp(i) + math.pow((y(i) / (lambda * t)), 2) * inner_prod((i, i))
          alpha = sample(i)._2._2
          local_set = local_set + (sample(i)._1 ->(sample(i)._2._1, alpha + (1 / (lambda * t * s))))

          for (j <- (i + 1) to (minibatchSize - 1)) {
            yp(j) = yp(j) + y(j) / (lambda * t) * inner_prod((i, j))
          }

          if (norm > (1 / lambda)) {
            s = s * (1 / math.sqrt(lambda * norm))
            norm = (1 / lambda)
            for (j <- (i + 1) to (minibatchSize - 1)) {
              yp(j) = yp(j) / math.sqrt(lambda * norm)
            }
          }
        }
      }
      //batch update model
      //val to_forget = working_data

      //working_data = working_data.multiput(local_set).cache()
      //val localRDD = sc.parallelize(local_set.toSeq)
      val filtered = indexed_data.filter(f=>{!local_set.contains(f._1)}).collect()
      val array  = filtered++local_set.toArray[(Long,(LabeledPoint, Double))]
      indexed_data = sc.parallelize(array)
      //to_forget.unpersist()
      updateCount = updateCount + 1

      println("Iteration : " + t)

    }
    //model = working_data.map { case (k, v) => (v._1, v._2) }.filter { case (k, v) => (v > 0) }.cache()
    model = indexed_data.map { case (k, v) => (v._1, v._2) }.filter { case (k, v) => (v > 0) }.cache()
    //working_data.unpersist()

    new SVMNonLinearDualModel(kernelFunction, s, model)
  }

}