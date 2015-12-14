package org.apache.spark.ml.feature.reweighting

import org.apache.spark.mllib.feature.IDFWeightingSchema

/**
  * Created by wolf on 12.12.2015.
  */
class OddsRatio extends IDFWeightingSchema {
  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {

    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    Math.log(A * D / B * C)
  }
}
