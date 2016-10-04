package org.apache.spark.ml.pipes

import java.io.PrintWriter

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{SparseVector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by wolf on 26.06.2016.
  */

trait LibSVMSinkBase extends Params with HasInputCol with HasLabelCol with HasOutputCol with HasHandleInvalid {

  val trainFilename = new Param[String](this, "training filename", "training output LIBSVM filename")
  val testFilename = new Param[String](this, "test filename", "test output LIBSVM filename")
  val isTrain = new BooleanParam(this, "is train or test", "train/test output")

  def setTrainFilename(value: String): this.type = set(trainFilename, value)

  def setTestFilename(value: String): this.type = set(testFilename, value)

  def setIfTrain(value: Boolean): this.type = set(isTrain, value)


  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)


  setDefault(trainFilename -> "train-libsvm.txt", testFilename -> "test-libsvm.txt", isTrain -> true)

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

  def buildInstance(dataFrame: DataFrame): DataFrame = {
    val udfLine = udf {
      (features: mllib.linalg.Vector, label: Double) => {
        var line = label.toString
        features.foreachActive((index, value) => {
          line += " " + (index + 1).toString + ":" + value.toString
        })
        line
      }
    }

    dataFrame.select($(inputCol), $(labelCol))
      .withColumn($(outputCol), udfLine(col($(inputCol)), col($(labelCol))))

  }

  def aggregate(dataFrame: DataFrame): String = {
    val customAggregate = new TextAggregate()
    var mainText = dataFrame.select($(outputCol))
      .agg(customAggregate(dataFrame.col($(outputCol))).as("main-text"))
      .head().getString(0)

    mainText
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

}

class LibSVMSink(override val uid: String) extends Estimator[LibSVMSinkModel] with LibSVMSinkBase {
  //Tranform features column, label column to weka arff line string in dataframe
  //Build a header text for the labels, and features as model
  //Aggregate generated column into file including header text

  def this() = this(Identifiable.randomUID("libsvm-sink"))

  override def fit(dataset: DataFrame): LibSVMSinkModel = {


    new LibSVMSinkModel(uid)
      .setLabelCol($(labelCol))
      .setOutputCol($(outputCol))
      .setInputCol($(inputCol))
      .setTrainFilename($(trainFilename))
      .setTestFilename($(testFilename))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)


  override def copy(extra: ParamMap): Estimator[LibSVMSinkModel] = defaultCopy(extra)


}

class LibSVMSinkModel(override val uid: String) extends Model[LibSVMSinkModel] with LibSVMSinkBase with MLWritable {

  var toggle = true

  def this() = this(Identifiable.randomUID("libsvm-sinkmodel"))

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = ???

  override def transform(dataset: DataFrame): DataFrame = {
    val df = buildInstance(dataset)
    aggregateToFile(df)
    df
  }

  protected def aggregateToFile(dataset: DataFrame): Unit = {
    val filename = if (toggle) {
      toggle = false
      $(trainFilename)

    } else {
      toggle = true
      $(testFilename)
    }

    val libSVMText = aggregate(dataset)
    new PrintWriter(filename) {
      write(libSVMText)
    }.close()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def copy(extra: ParamMap): LibSVMSinkModel = {
    val copied = new LibSVMSinkModel(uid)
    copyValues(copied, extra).setParent(parent)
  }
}
