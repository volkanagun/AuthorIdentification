package org.apache.spark.ml.pipes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MetadataUtils, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
  * Created by wolf on 17.09.2016.
  */

trait PipesLogisticParams extends Serializable with HasFeaturesCol with HasLabelCol with HasPredictionCol with HasTol with HasMaxIter {

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

}

class PipesLogistic(override val uid: String) extends Estimator[PipesLogisticModel] with PipesLogisticParams {

  def this() = this(Identifiable.randomUID("logistic-classifier"))

  protected def computeNumClasses(dataset: DataFrame): Int = {
    println("Computing number of classes in the dataset..")
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
  }

  override def fit(dataset: DataFrame): PipesLogisticModel = {
    val rdd = dataset.select($(featuresCol), $(labelCol)).map(row => {
      LabeledPoint(row.getDouble(1), row.getAs[mllib.linalg.Vector](0))
    })
    val numClasses = computeNumClasses(dataset)
    val model = new LogisticRegressionWithLBFGS()
      .setValidateData(false)
      .setNumClasses(numClasses)
      .run(rdd)

    new PipesLogisticModel(model)
      .setParent(this)
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
  }

  override def copy(extra: ParamMap): Estimator[PipesLogisticModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

class PipesLogisticModel(override val uid: String, val model: LogisticRegressionModel) extends Model[PipesLogisticModel] with HasPredictionCol with HasFeaturesCol {
  def this(model: LogisticRegressionModel) = this(Identifiable.randomUID("logistic-model"), model)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def copy(extra: ParamMap): PipesLogisticModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    val func = udf {
      features: mllib.linalg.Vector => {
        model.predict(features)
      }
    }

    val schema = transformSchema(dataset.schema)
    val metadata = schema($(predictionCol)).metadata
    dataset.withColumn($(predictionCol), func(col($(featuresCol))), metadata)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}