package org.apache.spark.ml.classification

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.{Estimator, Model, PredictorParams}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils, SchemaUtils}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuilder

/**
  * Created by wolf on 28.06.2016.
  */

private[ml] trait OneVsOneParams extends PredictorParams {

  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }

  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")
  val classBased = new BooleanParam(this, "classBased", "class based weighting")
  val weightedCols = new StringArrayParam(this, "weightedColumns", "feature columns that are weighted")
  val staticCol = new Param[String](this, "staticColumn", "features inside static column")

  setDefault(staticCol -> null, weightedCols -> null, classBased -> false)

  def getClassifier: ClassifierType = $(classifier)

  def getClassBased: Boolean = $(classBased)

  def getWeigtedCols: Array[String] = $(weightedCols)

  def getStaticCol: String = $(staticCol)

  def setClassBased(value: Boolean): this.type = set(classBased, value)

  def setWeightedCols(value: Array[String]): this.type = set(weightedCols, value)

  def setStaticCol(value: String): this.type = set(staticCol, value)

  def hasColumn(schema:StructType, colName:String):Boolean={
    val array = schema.map(p=>{p.name}).toArray
    array.contains(colName)
  }

  def prepare(df: DataFrame, numClasses: Int, classIndex: Int): DataFrame = {
    println("Data preparation is started...")
    val colName = $(featuresCol)
    val hasStaticColumn = hasColumn(df.schema, getStaticCol)
    val slice = udf((features: Row) => {
      sliceByClass(features, numClasses, classIndex, hasStaticColumn)
    })

    val attrs = attrByClass(df, numClasses, classIndex)

    val cols = if(hasStaticColumn) getWeigtedCols :+ getStaticCol else getWeigtedCols
    val args = cols.map(c => col(c))


    val metadata = new AttributeGroup(colName, attrs).toMetadata()

    df.drop(colName)
      .select(col("*"), slice(struct(args: _*)).as(colName, metadata))
  }


  def attrByClass(df: DataFrame, numClasses: Int, classIndex: Int): Array[Attribute] = {

    val schema = df.schema
    lazy val first = df.first()

    var attrs: Array[Attribute] = getWeigtedCols.flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case DoubleType =>
          val attr = Attribute.fromStructField(field)
          // If the input column doesn't have ML attribute, assume numeric.
          if (attr == UnresolvedAttribute) {
            Some(NumericAttribute.defaultAttr.withName(c))
          } else {
            Some(attr.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Some(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            val attributes = group.attributes.get
            val vecSize = attributes.size
            val jump = vecSize / numClasses
            val start = classIndex * jump
            val end = start + jump
            val arr = attributes.slice(start, end)
            arr.zipWithIndex.map { case (attr, i) =>
              if (attr.name.isDefined) {
                // TODO: Define a rigorous naming scheme.
                attr.withName(c + "_" + attr.name.get)
              } else {
                attr.withName(c + "_" + i)
              }
            }
          } else {
            // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
            // from metadata, check the first row.
            val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
            Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(c + "_" + i))
          }
        case otherType =>
          throw new SparkException(s"VectorAssembler does not support the $otherType type")
      }
    }

    if(hasColumn(schema, getStaticCol)) {
      val c = getStaticCol
      val field = schema(c)
      val group = AttributeGroup.fromStructField(field)
      val index = schema.fieldIndex(c)
      val statAttrs = if (group.attributes.isDefined) {
        // If attributes are defined, copy them with updated names.
        val attributes = group.attributes.get
        attributes.zipWithIndex.map { case (attr, i) =>
          if (attr.name.isDefined) {
            // TODO: Define a rigorous naming scheme.
            attr.withName(c + "_" + attr.name.get)
          } else {
            attr.withName(c + "_" + i)
          }
        }
      } else {
        // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
        // from metadata, check the first row.
        val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
        Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(c + "_" + i))
      }


      attrs = attrs ++ statAttrs
    }

    attrs

  }

  def sliceByClass(features: Row, numClasses: Int, i: Int, hasStaticColumn:Boolean): Vector = {
    var cur = 0
    val indices = ArrayBuilder.make[Int]()
    val values = ArrayBuilder.make[Double]()
    getWeigtedCols.zipWithIndex.foreach { case (column, index) => {

      val vec = features.getAs[Vector](index)
      val vecSize = vec.size
      val jump = vecSize / numClasses
      val start = i * jump
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

    val staticIndex = getWeigtedCols.size
    if (hasStaticColumn) {
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

    val indiceArray = indices.result()
    val valueArray = values.result()

    Vectors.sparse(cur,indiceArray , valueArray).compressed
  }

  //<editor-fold desc="Slicing Operation">
  /*def sliceByRegular(features: Row): Vector = {
    val indices = ArrayBuilder.make[Int]()
    val values = ArrayBuilder.make[Double]()
    var cur = 0
    getWeigtedCols.zipWithIndex.foreach { case (column, index) => {
      val vec: SparseVector = features.getAs[SparseVector](index)
      val vecSize = vec.size

      vec.foreachActive { case (i, v) => {
        if (v != 0.0) {
          indices += cur + i
          values += v
        }
      }
      }

      cur += vecSize
    }
    }

    val staticIndex = getWeigtedCols.length

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

    Vectors.sparse(cur, indices.result(), values.result())
  }*/

  /*def attrByRegular(df: DataFrame): Array[Attribute] = {
    val schema = df.schema
    lazy val first = df.first()
    val allCols = Array(getFeaturesCol)
    allCols.flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case DoubleType =>
          val attr = Attribute.fromStructField(field)
          // If the input column doesn't have ML attribute, assume numeric.
          if (attr == UnresolvedAttribute) {
            Some(NumericAttribute.defaultAttr.withName(c))
          } else {
            Some(attr.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Some(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            group.attributes.get.zipWithIndex.map { case (attr, i) =>
              if (attr.name.isDefined) {
                // TODO: Define a rigorous naming scheme.
                attr.withName(c + "_" + attr.name.get)
              } else {
                attr.withName(c + "_" + i)
              }
            }
          } else {
            // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
            // from metadata, check the first row.
            val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
            Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(c + "_" + i))
          }
        case otherType =>
          throw new SparkException(s"VectorAssembler does not support the $otherType type")
      }
    }
  }*/
  //</editor-fold>

}

class OneVsOne(override val uid: String)
  extends Estimator[OneVsOneModel]
    with OneVsOneParams {
  def this() = this(Identifiable.randomUID("oneVsOne"))

  def setClassifier(value: Classifier[_, _, _]): this.type = set(classifier, value.asInstanceOf[ClassifierType])

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)


  override def transformSchema(schema: StructType): StructType = {
    if(getClassBased) {
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
      SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
    }
    else{
      SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
      SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
    }
  }



  override def fit(dataset: DataFrame): OneVsOneModel = {
    // determine number of classes either from metadata if provided, or via computation.


    println("Building OneVsOne models...")

    val labelSchema = dataset.schema($(labelCol))

    println("Computing number of classes in the dataset..")
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)

    println("Filtering label and features column...")
    val filterCols: Array[String] = if (!getClassBased) {
      getWeigtedCols :+ getFeaturesCol :+ getLabelCol
    } else if(hasColumn(dataset.schema, getStaticCol)){
      getWeigtedCols :+ getStaticCol :+ getLabelCol
    }
    else{
      getWeigtedCols :+ getLabelCol
    }

    val multiclassLabeled = dataset.select(filterCols.head, filterCols.tail: _*)

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val pairs = for {
      i <- 0 until numClasses - 1
      j <- i + 1 until numClasses
    } yield (i, j)

    // create k columns, one for each binary classifier.
    val models = pairs.zipWithIndex.map { case ((i, j), index) =>
      // generate new label metadata for the binary problem.
      println(s"Learning model $i and $j ... with index $index/${pairs.length}")
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelColName = "mc2b$" + i
      val trainingDataset = multiclassLabeled.filter(col($(labelCol)) === i.toDouble || col($(labelCol)) === j.toDouble).withColumn(
        labelColName, when(col($(labelCol)) === i.toDouble, 1.0)
          .otherwise(0.0), newLabelMeta)

      val preparedDataset = if (getClassBased) {
        trainingDataset.printSchema()
        prepare(trainingDataset, numClasses, i)
      } else {
        trainingDataset
      }

      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)
      (index, classifier.fit(preparedDataset, paramMap))
    }.toArray[(Int, ClassificationModel[_, _])]
      .sortBy(pair => pair._1)
      .map(pair => pair._2)

    //toArray[ClassificationModel[_, _]]

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
    val model = new OneVsOneModel(uid, labelAttribute.toMetadata(), models)
      .setParent(this)

    copyValues(model)
  }


  override def copy(extra: ParamMap): OneVsOne = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsOne]
    if (isDefined(classifier)) {
      val classy = $(classifier)
      copied.setClassifier(classy.copy(extra))
    }
    copied
  }

}

final class OneVsOneModel private[ml](override val uid: String, labelMetadata: Metadata, val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[OneVsOneModel] with OneVsOneParams {

  //def setClassifier(value:ClassifierType): this.type  = set(classifier,value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }

  override def copy(extra: ParamMap): OneVsOneModel = {
    val copied = new OneVsOneModel(
      uid, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: DataFrame): DataFrame = {


    val sql = dataset.sqlContext

    println("OneVsOne Transformation is started...")

    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val initUDF = udf { () => Map[Int, Double]() }

    val newDataset = dataset.withColumn(accColName, initUDF())


    val pairs = for {
      i <- 0 until numClasses - 1
      j <- i + 1 until numClasses
    } yield (i, j)

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      println("Persistence...")
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    var aggregatedDF = newDataset
    println(s"OneVsOne aggregation is started...for ${pairs.length} number of pairs")

    pairs.zipWithIndex.foreach { case ((i, j), index) => {
      val prepDF = if (getClassBased) prepare(aggregatedDF, numClasses, i) else aggregatedDF
      val model = models(index)
      val rawPredictionCol = model.getRawPredictionCol
      val columns = origCols ++ List(col(rawPredictionCol), col(accColName))
      val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
      val newCols = origCols ++ List(col(tmpColName))

      val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
        if (prediction(1) >= 1.0) {
          predictions.updated(i, predictions.getOrElse(i, 0.0) + 1.0)
        }
        else {
          predictions.updated(j, predictions.getOrElse(j, 0.0) + 1.0)
        }
      }
      println(s"Transforming $i and $j pairs with index $index")

      val transDF = model.transform(prepDF)
        .select(columns: _*)


      val updatedDF = transDF
        .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))

      val newDF = updatedDF.select(newCols: _*)
        .withColumnRenamed(tmpColName, accColName)

      aggregatedDF = newDF
      /*aggregatedDF.persist(StorageLevel.MEMORY_ONLY)
      aggregatedDF.count()*/

      //aggregatedDF.explain()
    }
    }



    /*val aggregatedDF = pairs.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, ((i, j), index)) => {
        val prepDF = if(getClassBased) prepare(df, numClasses, i) else df
        val model = models(index)
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val newCols = origCols ++ List(col(tmpColName))

        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          if (prediction(1) >= 0.0) predictions.updated(i, predictions.getOrElse(i, 0.0) + 1)
          else predictions.updated(j, predictions.getOrElse(j, 0.0) + 1.0)
        }



        val transDF = model.transform(prepDF).select(columns: _*)
          .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
          .select(newCols: _*).withColumnRenamed(tmpColName, accColName)

        println(s"Transforming $i and $j pairs with index $index")

        //Action...
        //transDF.count()

        transDF
      }
    }*/

    if (handlePersistence) {
      newDataset.unpersist()
    }


    val labelUDF = udf { (predictions: Map[Int, Double]) =>
      predictions.maxBy(_._2)._1.toDouble
    }

    // output label and label metadata as prediction
    val outputDF = aggregatedDF.withColumn($(predictionCol), labelUDF(col(accColName)), labelMetadata)
      .drop(accColName)

    //Action

    outputDF
  }
}
