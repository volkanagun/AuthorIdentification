package org.apache.spark.ml.classification

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.bases.{MultiPredictionModel, MultiPredictor}
import org.apache.spark.mllib.MultiLabeledPoint
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by wolf on 01.03.2016.
  */
@Since("1.5.0")
@Experimental
class MultiLabelSVMClassifier @Since("1.5.0")(@Since("1.5.0") override val uid: String)
  extends MultiPredictor[Vector, MultiLabelSVMClassifier, MultiLabelSVMModel] with HasMaxIter  {

  def this() = this("multilabel-svm")

  val labelSize: IntParam = new IntParam(this, "number of labels", "Total number of labels", ParamValidators.gtEq(0.0))

  @Since("1.6.0")
  def getLabelSize: Double = $(labelSize)

  @Since("1.6.0")
  def setLabelSize(size: Int) = set(labelSize, size)

  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)


  override def copy(extra: ParamMap): MultiLabelSVMClassifier = defaultCopy(extra)

  /**
    * Train a model using the given dataset and parameters.
    * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
    * and copying parameters into the model.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: DataFrame): MultiLabelSVMModel = {
    val niter = $(maxIter)
    val lpData = extractLabeledPoints(dataset)
    println("Mapping and collecting data as multilabel dataset")
    val data = mapData(lpData)
    val keys = data.keys
      .distinct()
      .sortBy(index=>index)
      .collect()

    val modelByIndex = keys.par.map(ii=>{
      val rdd = data.filter{case(index,lp)=>{ index==ii }}.values.cache()
      println("Training model "+ii+ " over "+ keys.length)
      (ii,SVMWithSGD.train(rdd,niter))
    }).seq

    val models = modelByIndex.sortBy(pair=>pair._1).map(pair=>pair._2)

    new MultiLabelSVMModel(models)
      .setFeaturesCol($(featuresCol)).setPredictionCol($(predictionCol))
  }

  protected def mapData(data: RDD[MultiLabeledPoint]): RDD[(Int, LabeledPoint)] = {
    data.flatMap(lp => {
      val features = lp.features
      val labels = lp.label.toArray
      labels.zipWithIndex.map{ case (target: Double, index: Int) => {
        (index, LabeledPoint(target, features))
      }}
    })
  }
}

@Since("1.6.0")
@Experimental
class MultiLabelSVMModel private[ml](@Since("1.6.0") override val uid: String, @Since("1.6.0") val models: Seq[SVMModel]) extends MultiPredictionModel[Vector, MultiLabelSVMModel]
  with Serializable with Saveable  {
  /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */

  def this(models: Seq[SVMModel]) = this("multilabel-svm", models)



  /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */
  override protected def predict(features: Vector): Vector = {
    //Predict each label by it's model
    //Mark result vector as 1 or 0 for that index
    val predictions = Array.fill[Double](models.length)(0.0)
    models.zipWithIndex.foreach { case (model: SVMModel, index: Int) => {
      val predicted = model.predict(features)
      predictions(index) = if(predicted>0) index+1 else 0
    }}

    val output =  predictions.filter(value => value > 0)
      .map(value => value - 1)

    Vectors.dense(output)
  }


  override protected def formatVersion: String = "1.6.0"

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    models.zipWithIndex.foreach { case (model: SVMModel, index: Int) => {
      val filename = path + "/" + index
      model.save(sc, filename)
    }
    }
  }

  override def copy(extra: ParamMap): MultiLabelSVMModel = {
    copyValues(new MultiLabelSVMModel(uid, models), extra)
  }
}

object MultiLabelSVMModel extends Loader[MultiLabelSVMModel] {
  @Since("1.3.0")
  override def load(sc: SparkContext, path: String): MultiLabelSVMModel = {
    val filenames = (new File(path)).listFiles()
      .filter(f => f.isDirectory)
      .sortBy(f => f.getName.toInt)
      .map(f => f.getAbsolutePath)

    val models = filenames.map(filepath => {
      println("Loading model from "+filepath)
      SVMModel.load(sc, filepath)
    })


    new MultiLabelSVMModel(models)
  }
}