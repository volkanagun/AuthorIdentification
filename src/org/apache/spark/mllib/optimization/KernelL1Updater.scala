package org.apache.spark.mllib.optimization

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.classification.KernelFunction
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Vectors, BLAS, Vector}
import breeze.linalg.{norm => brzNorm, axpy => brzAxpy, Vector => BV}

import scala.math._

/**
  * Created by wolf on 24.12.2015.
  */

@DeveloperApi
abstract class KernelizedUpdater extends Serializable {
  /**
    * Compute an updated value for weights given the gradient, stepSize, iteration number and
    * regularization parameter. Also returns the regularization value regParam * R(w)
    * computed using the *updated* weights.
    *
    * @param weightsOld - Column matrix of size dx1 where d is the number of features.
    * @param gradient - Column matrix of size dx1 where d is the number of features.
    * @param stepSize - step size across iterations
    * @param iter - Iteration number
    * @param regParam - Regularization parameter
    *
    * @return A tuple of 2 elements. The first element is a column matrix containing updated weights,
    *         and the second element is the regularization value computed using updated weights.
    */
  def compute(
               weightsOld: Vector,
               gradient: Vector,
               intercept:Double,
               stepSize: Double,
               iter: Int,
               regParam: Double): (Vector, Double)
}

class KernelL1Updater(val kernel:KernelFunction) extends KernelizedUpdater{


  override def compute(weightsOld: Vector, gradient: Vector, intercept:Double, stepSize: Double, iter: Int, regParam: Double): (Vector, Double) = {

    val r = stepSize / math.sqrt(iter) * regParam
    BLAS.scal(r, weightsOld)

    val b = intercept + r * intercept
    val w:BV[Double] = weightsOld.toBreeze.toDenseVector


    val reg = kernel.regularizerGradient(weightsOld)
    for(i<-0 until w.length)
    {
      val wi = w(i)
      val wiabs = Math.abs(wi)
      if(wiabs>0) {
        w(i) = wi / wiabs * Math.max(0, wiabs - r * reg(i))
      }
    }

    (Vectors.fromBreeze(w), b)
  }
}

@DeveloperApi
class L1UpdaterKernel extends KernelizedUpdater {
  override def compute(
                        weightsOld: Vector,
                        gradient: Vector,
                        intercep:Double,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    // Take gradient step
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam * thisIterStepSize
    var i = 0
    val len = brzWeights.length
    while (i < len) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
      i += 1
    }

    (Vectors.fromBreeze(brzWeights), brzNorm(brzWeights, 1.0) * regParam)
  }
}