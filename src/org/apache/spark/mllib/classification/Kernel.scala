package org.apache.spark.mllib.classification

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}

/**
  * Created by wolf on 22.12.2015.
  */

trait KernelFunction extends Serializable{
  def compute(x:Vector, weights:Vector):Double
  def gradient(x:Vector, weights:Vector):Vector
  def regularizer(weights:Vector) : Double
  def regularizerGradient(weights:Vector):Vector

}

class RBFKernel(val gamma:Double) extends KernelFunction
{
  override def compute(x: Vector, weights: Vector): Double = {
    require(x.size==weights.size, "Size of two vectors must be equal...")
    val scale = 1/gamma * BLAS.dot(x, weights)
    math.exp(scale)
  }

  override def gradient(x: Vector, weights: Vector): Vector = {
    require(x.size==weights.size, "Size of two vectors must be equal...")
    val product = compute(x,weights)
    val gradient = x.copy
    BLAS.scal(product/gamma, gradient)
    gradient
  }

  override def regularizer(weights:Vector): Double = {
    compute(weights,weights)
  }

  override def regularizerGradient(weights: Vector): Vector = {
    val gradient = weights.copy
    BLAS.scal(2*compute(weights,weights)/gamma, gradient)
    gradient
  }

}


