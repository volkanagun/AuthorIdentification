package org.apache.spark.mllib

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector

import scala.beans.BeanInfo

/**
  * Class that represents the features and labels of a data point.
  *
  * @param label Label for this data point.
  * @param features List of features for this data point.
  */
@Since("0.8.0")
@BeanInfo
case class MultiLabeledPoint @Since("1.0.0")(@Since("0.8.0") label: Vector,
                                             @Since("1.0.0") features: Vector) {

  override def toString: String = {
    s"($label,$features)"
  }
}

