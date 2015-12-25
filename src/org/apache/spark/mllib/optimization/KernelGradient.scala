package org.apache.spark.mllib.optimization

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.classification.{RBFKernel, KernelFunction}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg._

/**
  * Created by wolf on 20.12.2015.
  */
@DeveloperApi
abstract class KernelizedGradient extends Serializable{
  /**
    * Compute the gradient and loss given the features of a single data point.
    *
    * @param data features for one data point
    * @param label label for this data point
    * @param weights weights/coefficients corresponding to features
    *
    * @return (gradient: Vector, loss: Double)
    */
  def compute(data: Vector, label: Double, weights: Vector, intercept:Double): (Vector, Double) = {
    val gradient = Vectors.zeros(weights.size)
    val loss = compute(data, label, weights, intercept, gradient)
    (gradient, loss)
  }

  /**
    * Compute the gradient and loss given the features of a single data point,
    * add the gradient to a provided vector to avoid creating new objects, and return loss.
    *
    * @param data features for one data point
    * @param label label for this data point
    * @param weights weights/coefficients corresponding to features
    * @param cumGradient the computed gradient will be added to this vector
    *
    * @return loss
    */
  def compute(data: Vector, label: Double, weights: Vector,intercept:Double, cumGradient: Vector): Double

}

@DeveloperApi
class KernelGradient(val kernel: KernelFunction, val threshold:Double) extends KernelizedGradient {

  override def compute(data: Vector, label: Double, weights: Vector, intercept:Double): (Vector, Double) = {

    val kernelProduct = kernel.compute(data, weights)


    if (label * (kernelProduct + intercept) < threshold) {
      val grad = kernel.gradient(data, weights)
      val intercept = label
      (grad, intercept)
    }
    else{
      (Vectors.sparse(weights.size, Array.empty, Array.empty), 0.0)
    }
  }

  override def compute(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept:Double,
                        cumGradient: Vector): Double = {
    val kernelProduct = kernel.compute(data, weights)

    if (label * (kernelProduct + intercept) < threshold) {
      val grad = kernel.gradient(data, weights)
      val intercept = label
      axpy(1, grad, cumGradient)
      intercept
    }
    else{
      0.0
    }
  }
}
