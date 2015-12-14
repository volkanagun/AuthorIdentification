package org.apache.spark.ml.feature.reweighting

import org.apache.spark.mllib.feature.IDFWeightingSchema

/**
  * Created by wolf on 12.12.2015.
  */
class CorrCoefficient extends IDFWeightingSchema {
  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d

    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    Math.sqrt(N) * (A * D - B * C) / Math.sqrt((A + B) * (B + D) * (A + B) * (C + D))
  }
}
