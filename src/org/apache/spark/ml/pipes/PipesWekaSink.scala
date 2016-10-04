package org.apache.spark.ml.pipes


import java.io.PrintWriter

import breeze.linalg
import data.dataset.DatasetStats
import options.{ParamsUnique, ParamsWriter}
import org.apache.spark.sql.functions._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter, MetadataUtils}
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.tools.nsc.javac.JavaTokens

/**
  * Created by wolf on 08.06.2016.
  */
trait WekaSinkBase extends Params with HasInputCol with HasLabelCol with HasOutputCol with HasHandleInvalid {

  val trainFilename = new Param[String](this, "training filename", "training output ARFF filename")
  val testFilename = new Param[String](this, "testing filename", "testing output ARFF filename")
  val fullFilename = new Param[String](this, "full filename", "testing and training combined output ARFF filename")

  def setTrainFilename(value: String): this.type = set(trainFilename, value)

  def setTestFilename(value: String): this.type = set(testFilename, value)

  def setFullFilename(value: String): this.type = set(fullFilename, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)


  setDefault(trainFilename -> "train.arff", testFilename -> "test.arff")

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == new VectorUDT(),
      s"The input column $inputColName must be vector type " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  protected def fixAttrName(name: String): String = {
    var mname = name.replaceAll("\\'", "-apostrophe-")
    mname = mname.replaceAll("\"", "-quoto-")
    mname = mname.replaceAll("\\,", "-comma-")
    mname = mname.replaceAll("\\%", "-percentage-")
    mname = mname.replaceAll("\\;", "-semicolon-")
    mname = mname.replaceAll("\\.", "-dot-")
    mname = mname.replaceAll("\\?", "-question-")
    mname = mname.replaceAll("\\n", "")
    mname = mname.replaceAll("\\s", "_")
    mname = mname.trim
    mname = if (mname.isEmpty) "non" else mname

    mname
  }

  def buildHeader(dataFrame: DataFrame): String = {
    val featuresSchema = dataFrame.schema($(inputCol))
    val metadata = AttributeGroup.fromStructField(featuresSchema)
    val head = dataFrame.select($(inputCol)).head()

    val featureSize = head.get(0) match {
      case sv: SparseVector => sv.size
      case dv: DenseVector => dv.size
      case _ => 0
    }

    /*val labelSize = dataFrame.select($(labelCol)).agg(max($(labelCol))).head().getAs[Double](0) + 1*/
    val labels = dataFrame.select($(labelCol))
      .distinct()
      .map(row=>row.getString(0))
      .collect()

    if (featureSize != metadata.attributes.get.length) {
      throw new IndexOutOfBoundsException("Attribute size is not equal the feature size!")
    }

    println("Attribute header building is started...")
    var text = metadata.attributes.get.zipWithIndex.foldLeft[String]("@RELATION datasink\n") { case (crrText, (attribute, index)) => {
      val name = fixAttrName(attribute.name.get) + index.toString
      crrText + "@ATTRIBUTE att_" + name + " NUMERIC\n"
    }
    }

    println("Class header building is started...")

    val classText = (0 until labels.size).foldLeft[String]("") {
      (crrText, i) => {
        crrText + "," + buildClass(labels(i))
      }
    }

    text += "@ATTRIBUTE class{" + classText.substring(1) + "}\n"
    text += "@DATA\n"

    text
  }

  /*def buildClass(label: Int): String = {
    "class" + label.toInt.toString
  }*/

  def buildClass(label: String): String = {
    "class_" + label.replaceAll("\\s","_")
  }

  def buildDataFrame(dataFrame: DataFrame): DataFrame = {
    val udfLine = udf {
      (features: mllib.linalg.Vector, label: String) => {
        var line = ""
        features.foreachActive((index, value) => {
          line += index.toString + " " + value + ", "
        })

        "{" + line + (features.size) + " " + buildClass(label) + "}"
      }
    }

    dataFrame.select(col("*"))
      .withColumn($(outputCol), udfLine(col($(inputCol)), col($(labelCol))))
  }

  def aggregate(dataFrame: DataFrame): String = {
    val customAggregate = new TextAggregate()
    val mainText = dataFrame.select($(outputCol))
      .agg(customAggregate(dataFrame.col($(outputCol))).as("main-text"))
      .head().getString(0)

    mainText
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}

class TextAggregate extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("text", StringType)))

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = (buffer.getString(0) + "\n" + input.getString(0)).trim
  }

  override def bufferSchema: StructType = StructType(Array(StructField("main-text", StringType)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = (buffer1.getString(0) + "\n" + buffer2.getString(0)).trim
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }

  override def dataType: DataType = StringType
}

class WekaSink(override val uid: String) extends Estimator[WekaSinkModel] with WekaSinkBase {
  //Tranform features column, label column to weka arff line string in dataframe
  //Build a header text for the labels, and features as model
  //Aggregate generated column into file including header text

  def this() = this(Identifiable.randomUID("weka-sink"))

  override def fit(dataset: DataFrame): WekaSinkModel = {
    val headerText = buildHeader(dataset)
    new WekaSinkModel(uid, headerText)
      .setLabelCol($(labelCol))
      .setOutputCol($(outputCol))
      .setInputCol($(inputCol))
      .setTrainFilename($(trainFilename))
      .setTestFilename($(testFilename))
      .setFullFilename($(fullFilename))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)


  override def copy(extra: ParamMap): Estimator[WekaSinkModel] = defaultCopy(extra)


}

class WekaSinkModel(override val uid: String, headerText: String) extends Model[WekaSinkModel] with WekaSinkBase with MLWritable {
  var toggle = true

  def this(headerText: String) = this(Identifiable.randomUID("weka-sink"), headerText)

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = ???

  override def transform(dataset: DataFrame): DataFrame = {
    val df = buildDataFrame(dataset)
    aggregateToFile(df)
    df
  }

  def aggregateToFile(dataset: DataFrame): Unit = {
    val filename = if (toggle) {
      toggle = false
      $(trainFilename)
    }
    else {
      toggle = true
      $(testFilename)
    }
    //check before executing the pipeline
    FileProcess.createFolder(filename)
    println("Aggregating features to file with header text...")
    val arffText = headerText + aggregate(dataset)
    new PrintWriter(filename) {
      write(arffText)
    }.close()

  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def copy(extra: ParamMap): WekaSinkModel = {
    val copied = new WekaSinkModel(uid, headerText)
    copyValues(copied, extra).setParent(parent)
  }
}

class WekaSample(override val uid: String) extends Estimator[WekaSampleModel] {
  //the parameters should be sink to file
  //like in the evalution train and test should not be seperated
  def this() = this(Identifiable.randomUID("weka-stats"))

  val trainStatsFilename = new Param[String](this, "trainStatsFilename", "Training document statistics filename")
  val testStatsFilename = new Param[String](this, "testStatsFilename", "Test document statistics filename")

  val testSampleFilename = new Param[String](this, "testSampleFilename", "Test document sampling filename")
  val trainSampleFilename = new Param[String](this, "trainSampleFilename", "Train  document sampling filename")

  def setTrainStats(value: String): this.type = set(trainStatsFilename, value)

  def setTestStats(value: String): this.type = set(testStatsFilename, value)

  def setTrainSample(value: String): this.type = set(trainSampleFilename, value)

  def setTestSample(value: String): this.type = set(testSampleFilename, value)

  override def fit(dataset: DataFrame): WekaSampleModel = {
    new WekaSampleModel(uid)
      .setTrainStats($(trainStatsFilename))
      .setTestStats($(testStatsFilename))
      .setTrainSample($(trainSampleFilename))
      .setTestSample($(testSampleFilename))
  }


  override def copy(extra: ParamMap): Estimator[WekaSampleModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

}

class WekaSampleModel(override val uid: String) extends Model[WekaSampleModel] {

  var toggle = true
  val trainStatsFilename = new Param[String](this, "trainStatsFilename", "Training document statistics filename")
  val testStatsFilename = new Param[String](this, "testStatsFilename", "Test document statistics filename")

  val testSampleFilename = new Param[String](this, "testSampleFilename", "Test document sampling filename")
  val trainSampleFilename = new Param[String](this, "trainSampleFilename", "Train  document sampling filename")

  def setTrainStats(value: String): this.type = set(trainStatsFilename, value)

  def setTestStats(value: String): this.type = set(testStatsFilename, value)

  def setTrainSample(value: String): this.type = set(trainSampleFilename, value)

  def setTestSample(value: String): this.type = set(testSampleFilename, value)


  override def transform(dataset: DataFrame): DataFrame = {
    if (toggle) {
      toggle = false
      val sampleFilename = $(trainSampleFilename)
      val statsFilename = $(trainStatsFilename)

      ParamsWriter.writeAsXML(sampleFilename)
      DatasetStats.writeAsXML(dataset, statsFilename)
    }
    else {
      toggle = true
      val sampleFilename = $(testSampleFilename)
      val statsFilename = $(testStatsFilename)

      ParamsWriter.writeAsXML(sampleFilename)
      DatasetStats.writeAsXML(dataset, statsFilename)
    }
    dataset
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): WekaSampleModel = {
    val copied = new WekaSampleModel(uid)
    copyValues(copied, extra).setParent(parent)
  }
}

