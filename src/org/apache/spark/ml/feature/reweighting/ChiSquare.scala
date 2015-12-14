package org.apache.spark.ml.feature.reweighting

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.feature.IDFWeightingSchema


class ChiSquare extends IDFWeightingSchema {
  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d
    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    val nom = (A * D - B * C)
    N * (nom * nom) / ((A + C) * (B + D) * (A + B) * (C + D))
  }
}



















