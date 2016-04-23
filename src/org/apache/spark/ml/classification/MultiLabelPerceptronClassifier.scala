/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.classification.MultiLabelPerceptronClassificationModel.PerceptronModelWriter
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasMaxIter, HasSeed, HasTol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{MultiPredictionModel, MultiPredictor, MultiPredictorParams}
import org.apache.spark.mllib.MultiLabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Params for Multilayer Perceptron. */
private[ml] trait MultiLabelPerceptronParams extends MultiPredictorParams
  with HasSeed with HasMaxIter with HasTol {

  final val threshold: DoubleParam = new DoubleParam(this, "threshold", "Output of multilayer perceptron above threshold", ParamValidators.gtEq(0.0))

  final def getThreshold: Double = $(threshold)

  /**
    * Layer sizes including input size and output size.
    * Default: Array(1, 1)
    *
    * @group param
    */
  final val layers: IntArrayParam = new IntArrayParam(this, "layers",
    "Sizes of layers from input layer to output layer" +
      " E.g., Array(780, 100, 10) means 780 inputs, " +
      "one hidden layer with 100 neurons and output layer of 10 neurons.",
    // TODO: how to check ALSO that all elements are greater than 0?
    ParamValidators.arrayLengthGt(1)
  )

  /** @group getParam */
  final def getLayers: Array[Int] = $(layers)

  /**
    * Block size for stacking input data in matrices to speed up the computation.
    * Data is stacked within partitions. If block size is more than remaining data in
    * a partition then it is adjusted to the size of this data.
    * Recommended size is between 10 and 1000.
    * Default: 128
    *
    * @group expertParam
    */
  final val blockSize: IntParam = new IntParam(this, "blockSize",
    "Block size for stacking input data in matrices. Data is stacked within partitions." +
      " If block size is more than remaining data in a partition then " +
      "it is adjusted to the size of this data. Recommended size is between 10 and 1000",
    ParamValidators.gt(0))

  /** @group getParam */
  final def getBlockSize: Int = $(blockSize)

  setDefault(maxIter -> 100, tol -> 1e-4, layers -> Array(1, 1), blockSize -> 128, threshold -> 0.005)


}

/** Label to vector converter. */
private object MultiLabelConverter {
  // TODO: Use OneHotEncoder instead
  /**
    * Encodes a label as a vector.
    * Returns a vector of given length with zeroes at all positions
    * and value 1.0 at the position that corresponds to the label.
    *
    * @param labeledPoint labeled point
    * @param labelCount   total number of labels
    * @return pair of features and vector encoding of a label
    */
  def encodeLabeledPoint(labeledPoint: MultiLabeledPoint, labelCount: Int): (Vector, Vector) = {
    /*val output = Array.fill(labelCount)(0.0)
    labeledPoint.label.foreachActive { case (index, value) => {
      output(value.toInt) = 1.0
    }
    }*/

    val output = labeledPoint.label.toArray
    /*val modified = output.map(value => {
      if (value > 0.0) 1000.0
      else -1000.0
    })*/

    (labeledPoint.features.toDense, Vectors.dense(output))
  }

  /**
    * Converts a vector to a label.
    * Returns the position of the maximal element of a vector.
    *
    * @param output label encoded with a vector
    * @return label
    */
  def decodeLabel(output: Vector, threshold: Double): Vector = {
    val array = Array.fill(output.size)(0.0)
    output.foreachActive { case (index, value) => {
      if (value > threshold) {
        array(index) = index + 1
      }
    }}

    //Return indexes above a threshold
    val argmax = array
      .filter(value => value > 0)
      .map(value => value - 1)
    Vectors.dense(argmax)

  }
}

/**
  * :: Experimental ::
  * Classifier trainer based on the Multilayer Perceptron.
  * Each layer has sigmoid activation function, output layer has softmax.
  * Number of inputs has to be equal to the size of feature vectors.
  * Number of outputs has to be equal to the total number of labels.
  */
@Since("1.5.0")
@Experimental
class MultiLabelPerceptronClassifier @Since("1.5.0")(
                                                      @Since("1.5.0") override val uid: String)
  extends MultiPredictor[Vector, MultiLabelPerceptronClassifier, MultiLabelPerceptronClassificationModel]
    with MultiLabelPerceptronParams  {


  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mlpc"))

  /** @group setParam */
  @Since("1.5.0")
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /** @group setParam */
  @Since("1.5.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)


  @Since("1.5.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  /**
    * Set the maximum number of iterations.
    * Default is 100.
    *
    * @group setParam
    */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
    * Set the convergence tolerance of iterations.
    * Smaller value will lead to higher accuracy with the cost of more iterations.
    * Default is 1E-4.
    *
    * @group setParam
    */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
    * Set the seed for weights initialization.
    *
    * @group setParam
    */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultiLabelPerceptronClassifier = defaultCopy(extra)

  /**
    * Train a model using the given dataset and parameters.
    * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
    * and copying parameters into the model.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: DataFrame): MultiLabelPerceptronClassificationModel = {
    val myLayers = $(layers)
    val thresh = $(threshold)
    val labels = myLayers.last
    val lpData = extractLabeledPoints(dataset)
    val data = lpData.map(lp => MultiLabelConverter.encodeLabeledPoint(lp, labels))
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, false)
    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    /*FeedForwardTrainer.SGDOptimizer.setConvergenceTol($(tol))
      .setNumIterations($(maxIter)).setStepSize(1)*/

    FeedForwardTrainer.SGDOptimizer
      .setConvergenceTol($(tol))
      .setNumIterations($(maxIter))

    //.setNumCorrections(100)
    //.setUpdater(new SquaredL2Updater)
    //.setGradient(new LeastSquaresGradient)
    //FeedForwardTrainer.setStackSize($(blockSize))
    FeedForwardTrainer.setStackSize(1)
    val mlpModel = FeedForwardTrainer.train(data)
    new MultiLabelPerceptronClassificationModel(uid, myLayers, thresh, mlpModel.weights())
  }

}

/**
  * :: Experimental ::
  * Classification model based on the Multilayer Perceptron.
  * Each layer has sigmoid activation function, output layer has softmax.
  *
  * @param uid     uid
  * @param layers  array of layer sizes including input and output layers
  * @param weights vector of initial weights for the model that consists of the weights of layers
  * @return prediction model
  */
@Since("1.5.0")
@Experimental
class MultiLabelPerceptronClassificationModel private[ml](
                                                           @Since("1.5.0") override val uid: String,
                                                           @Since("1.5.0") val layers: Array[Int],
                                                           @Since("1.5.0") var threshold: Double,
                                                           @Since("1.5.0") val weights: Vector)
  extends MultiPredictionModel[Vector, MultiLabelPerceptronClassificationModel]
    with Serializable with MLWritable{

  @Since("1.6.0")
  override val numFeatures: Int = layers.head

  private val mlpModel = FeedForwardTopology.multiLayerPerceptron(layers, false).getInstance(weights)

  /**
    * Returns layers in a Java List.
    */
  private[ml] def javaLayers: java.util.List[Int] = {
    layers.toList.asJava
  }

  /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */
  override protected def predict(features: Vector): Vector = {
    val prediction = mlpModel.predict(features)
    MultiLabelConverter.decodeLabel(prediction, threshold)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultiLabelPerceptronClassificationModel = {
    copyValues(new MultiLabelPerceptronClassificationModel(uid, layers, threshold, weights), extra)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PerceptronModelWriter(this)
}

object MultiLabelPerceptronClassificationModel extends MLReadable[MultiLabelPerceptronClassificationModel] {


  @Since("1.6.0")
  override def read: MLReader[MultiLabelPerceptronClassificationModel] = new PerceptronModelReader

  @Since("1.6.0")
  override def load(path: String): MultiLabelPerceptronClassificationModel = super.load(path)

  /** [[MLWriter]] instance for [[MultiLabelPerceptronClassificationModel]] */
  private[MultiLabelPerceptronClassificationModel] class PerceptronModelWriter(instance: MultiLabelPerceptronClassificationModel) extends MLWriter {

    private case class Data(threshold: Double, layers: Array[Int], weights: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: pi, theta
      val data = Data(instance.threshold, instance.layers, instance.weights)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class PerceptronModelReader extends MLReader[MultiLabelPerceptronClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MultiLabelPerceptronClassificationModel].getName

    override def load(path: String): MultiLabelPerceptronClassificationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("threshold", "layers", "weights").head()
      val threshold = data.getAs[Double](0)
      val layers = data.getAs[mutable.WrappedArray[Int]](1)
      val weights = data.getAs[Vector](2)

      val model = new MultiLabelPerceptronClassificationModel(metadata.uid, layers.toArray, threshold, weights)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}
