package org.apache.spark.ml.classification

import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.shared.{HasMaxIter, HasSeed, HasTol}
import org.apache.spark.mllib

/**
  * Created by wolf on 22.07.2016.
  */
trait LogisticParams extends PredictorParams with HasSeed with HasMaxIter with HasTol {
  setDefault(tol -> 1e-4, maxIter -> 1000)
}
/*class LogisticClassifier  extends Classifier[mllib.linalg.Vector, SVMBinaryClassifier, SVMBinaryModel] with BinarySVMParams with Serializable {

}*/
