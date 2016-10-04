package org.apache.spark.ml.classification

import org.apache.spark.{ml, mllib}
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasMaxIter, HasSeed, HasTol}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Created by wolf on 26.06.2016.
  */

trait BinarySVMParams extends PredictorParams with HasSeed with HasMaxIter with HasTol {
  setDefault(tol -> 1e-4, maxIter -> 1000)

}

class SVMBinaryClassifier(override val uid: String) extends Classifier[mllib.linalg.Vector, SVMBinaryClassifier, SVMBinaryModel] with BinarySVMParams with Serializable {


  override def copy(extra: ParamMap): SVMBinaryClassifier = defaultCopy(extra)


  protected def train(dataset: DataFrame): SVMBinaryModel = {
    val lpPoints = extractLabeledPoints(dataset)
    val npoints = lpPoints.map(lp=> {
      val arr = lp.features.toArray
      val vec = mllib.linalg.Vectors.dense(arr)
      mllib.regression.LabeledPoint(lp.label,vec)
    })
    val m = SVMWithSGD.train(npoints, $(maxIter))
    new SVMBinaryModel(uid, m.weights, m.intercept)
  }

}

class SVMBinaryModel( override val uid: String,
                      val weight: mllib.linalg.Vector,
                      val b: Double) extends ClassificationModel[mllib.linalg.Vector, SVMBinaryModel] with Serializable {


  override def numClasses: Int = 2

  override def predict(v: mllib.linalg.Vector): Double = {
    val value = v.toArray.zip(weight.toArray).map { case (d1, d2) => d1 * d2 }.sum + b
    if (value > 0) 1 else 0
  }


  override protected def predictRaw(v: mllib.linalg.Vector): mllib.linalg.Vector = {
    val margin = v.toArray.zip(weight.toArray).map { case (d1, d2) => d1 * d2 }.sum + b
    Vectors.dense(margin, -margin)
  }

  override def copy(extra: ParamMap): SVMBinaryModel = {
    copyValues(new SVMBinaryModel(uid, weight, b), extra)
  }

}