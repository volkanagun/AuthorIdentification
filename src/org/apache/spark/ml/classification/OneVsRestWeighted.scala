package org.apache.spark.ml.classification

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Estimator, Model, PredictorParams}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MetadataUtils, SchemaUtils}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuilder

/**
  * Created by wolf on 04.05.2016.
  */
private[ml] trait WeightedParams extends PredictorParams {

  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type

  /**
    * param for the base binary classifier that we reduce multiclass classification into.
    * The base classifier input and output columns are ignored in favor of
    * the ones specified in [[WeightedClassifier]].
    *
    * @group param
    */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")

  val classBased = new BooleanParam(this, "classBased", "class based weighting")

  val weightedCols = new StringArrayParam(this, "weightedColumns", "feature columns that are weighted")

  val staticCol = new Param[String](this, "staticColumn", "features inside static column")


  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)

  def getClassBased: Boolean = $(classBased)

  def getWeigtedCols: Array[String] = $(weightedCols)

  def getStaticCol: String = $(staticCol)


  setDefault(staticCol -> null)

  //Modify dataset for all labels or only model index ???
  //Modify the dataset once select features only for label discard others
  //Create with a pipeline for test or use weighter model and weighter
  def modifyDataset(df: DataFrame, labelIndex: Int, numClasses: Int): DataFrame = {

    //combine all features for each column by using modelIndex, and numClasses
    val weightColumns = getWeigtedCols
    val allColumns = weightColumns :+ getStaticCol

    val isClassBased = getClassBased


    val args = allColumns.map { c => col(c) }

    val slice = udf((features: Row) => {

      val indices = ArrayBuilder.make[Int]()
      val values = ArrayBuilder.make[Double]()
      var cur = 0
      weightColumns.zipWithIndex.foreach { case (column, index) => {
        val vec = features.getAs[Vector](index)
        val vecSize = vec.size
        val jump = if (isClassBased) vecSize / numClasses else vecSize
        val start = if (isClassBased) labelIndex * jump else 0
        val end = start + jump
        val arr = vec.toArray.slice(start, end)

        arr.zipWithIndex.foreach { case (v, i) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }

        cur += jump
      }
      }

      val staticIndex = weightColumns.size
      if (getStaticCol != null) {
        val staticVec = features.getAs[Vector](staticIndex).toArray
        staticVec.zipWithIndex.foreach { case (v, i) => {
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }

        }
        cur += staticVec.length
      }


      Vectors.sparse(cur, indices.result(), values.result()).compressed
    })

    val colName = $(featuresCol)
    df.drop(colName).select(col("*"), slice(struct(args: _*)).as(colName))
  }
}

@Since("1.4.0")
@Experimental
final class WeightedModel private[ml](
                                       @Since("1.4.0") override val uid: String,
                                       @Since("1.4.0") numLabels: Int,
                                       @Since("1.4.0") labelMetadata: Metadata,
                                       @Since("1.4.0") val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[WeightedModel] with WeightedParams {

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }


  @Since("1.4.0")
  override def transform(dataset: DataFrame): DataFrame = {

    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)

    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val initUDF = udf { () => Map[Int, Double]() }
    val newDataset = dataset.withColumn(accColName, initUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }


    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) =>

        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          predictions + ((index, prediction(1)))
        }

        val modifiedDF = modifyDataset(newDataset, index, numLabels)

        val transformedDataset = model.transform(modifiedDF).select(columns: _*)
        val updatedDataset = transformedDataset
          .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))

        val newColumns = origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns: _*).withColumnRenamed(tmpColName, accColName)
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of the classifier with highest confidence as prediction
    val labelUDF = udf { (predictions: Map[Int, Double]) =>
      predictions.maxBy(_._2)._1.toDouble
    }

    // output label and label metadata as prediction
    aggregatedDataset
      .withColumn($(predictionCol), labelUDF(col(accColName)), labelMetadata)
      .drop(accColName)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): WeightedModel = {
    val copied = new WeightedModel(
      uid, numLabels, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
  }

}



@Since("1.4.0")
@Experimental
final class WeightedClassifier @Since("1.4.0")(@Since("1.4.0") override val uid: String)
  extends Estimator[WeightedModel] with WeightedParams {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  @Since("1.4.0")
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setClassBasedCol(value: Boolean): this.type = set(classBased, value)

  /** @group setParam */
  @Since("1.5.0")
  def setWeigtedCols(value: Array[String]): this.type = set(weightedCols, value)

  /** @group setParam */
  @Since("1.5.0")
  def setStaticCol(value: String): this.type = set(staticCol, value)


  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }

  @Since("1.4.0")
  override def fit(dataset: DataFrame): WeightedModel = {
    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
    val selectCols = getWeigtedCols.toSeq :+ getStaticCol :+ getLabelCol
    val multiclassLabeled = dataset.select(selectCols.head, selectCols.tail: _*)

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // create k columns, one for each binary classifier.
    val models = Range(0, numClasses).map { index =>
      // generate new label metadata for the binary problem.
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelColName = "mc2b$" + index
      //Modify features here
      val modifiedDataset = modifyDataset(multiclassLabeled, index, numClasses)
      //Modify only labels here
      val trainingDataset = modifiedDataset.withColumn(
        labelColName, when(col($(labelCol)) === index.toDouble, 1.0).otherwise(0.0), newLabelMeta)

      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)

      //modifiedDataset.show(2)

      classifier.fit(trainingDataset, paramMap)
    }.toArray[ClassificationModel[_, _]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    // extract label metadata from label column if present, or create a nominal attribute
    // to output the number of labels
    val labelAttribute = Attribute.fromStructField(labelSchema) match {
      case _: NumericAttribute | UnresolvedAttribute =>
        NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
      case attr: Attribute => attr
    }
    val model = new WeightedModel(uid, numClasses, labelAttribute.toMetadata(), models).setParent(this)
    copyValues(model)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): WeightedClassifier = {
    val copied = defaultCopy(extra).asInstanceOf[WeightedClassifier]

    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))

    }

    copied
  }
}


