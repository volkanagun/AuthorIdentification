package org.apache.spark.ml.feature.reweighting

import org.apache.spark.mllib.feature.IDFWeightingSchema

/**
  * Created by wolf on 12.12.2015.
  */
class TFIDF extends IDFWeightingSchema {
  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d
    Math.log(N / a)
  }
}
