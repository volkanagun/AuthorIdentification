package org.apache.spark.ml.feature.reweighting

import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.{VectorSlicer, IDFModel, IDFBase}
import org.apache.spark.ml.{Model, Estimator, Transformer}
import org.apache.spark.ml.param.{IntParam, Params, ParamMap}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.IDFWeightingSchema
import org.apache.spark.mllib.linalg.{Vectors, MatrixUDT, Vector, VectorUDT}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


private[feature] trait IDFClassBase extends Params with HasInputCol with HasLabelCol with HasOutputCol {

  /**
    * WeightingType parameter for different mechanisms
    */
  var weightingModel: IDFWeightingSchema = new TFIDF


  def getWeightingModel: IDFWeightingSchema = weightingModel

  def setWeightingModel(scheme: IDFWeightingSchema): this.type = {
    weightingModel = scheme
    return this
  }

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT, "Input column must be VectorUDT")
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType, "Label column must be double and sorted")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  protected def validateAndTransformSchema(schema: StructType, labels: Array[Double], termSize: Int): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT, "Input column must be VectorUDT")
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType, "Label column must be double and sorted")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

/**
  * Created by wolf on 01.12.2015.
  */
class IDFClass(override val uid: String) extends Estimator[IDFClassModel] with IDFClassBase {

  def this() = this(Identifiable.randomUID("tfidfclass"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)


  override def copy(extra: ParamMap): IDFClass = defaultCopy(extra)


  override def fit(df: DataFrame): IDFClassModel = {
    //Find distinct class labels means number of classes in sorted order
    //For each class label use hashing tf : count the number of occurrence of a term in each class
    //Find total number of terms |T|
    //Find total number of samples |N|

    //Find the number of documents of category c where term k appears at least once (A)
    //Find the number of documents that is not from category c and term k appears at least once (B)
    //Find the number of document of category c where term k doesn't appear (C)
    //Find the number of documents that is not from category c and term k doesn't appear (D)

    val outputSchema = transformSchema(df.schema, logging = true)
    val labelRDD = df.select($(labelCol)).distinct()
    val labels = labelRDD.map {
      case Row(l: Double) => l
    }.collect().sorted
    val input = df.select($(inputCol), $(labelCol)).map { case Row(v: Vector, l: Double) => (v, l) }
    val termSize = input.first()._1.size
    val idfc = new feature.IDFClass(weightingModel, labels, termSize).fit(input)
    copyValues(new IDFClassModel(uid, idfc).setParent(this))


  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


}

@Experimental
class IDFClassModel private[ml](
                                 override val uid: String,
                                 idfModel: feature.IDFClassModel)
  extends Model[IDFClassModel] with IDFClassBase {

  val vectorSlicer: VectorSlicer = new VectorSlicer()


  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val featureLength = dataset.select($(inputCol)).first().length
    val labelLength = idfModel.labels.length

    val idf = udf { vec: Vector => idfModel.transform(vec) }
    val ndf = dataset.withColumn($(outputCol), idf(col($(inputCol))))
    ndf
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, idfModel.labels, idfModel.termSize)
  }

  override def copy(extra: ParamMap): IDFClassModel = {
    val copied = new IDFClassModel(uid, idfModel)
    copyValues(copied, extra).setParent(parent)
  }
}

