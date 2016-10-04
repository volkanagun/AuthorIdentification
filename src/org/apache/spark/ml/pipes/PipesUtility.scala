package org.apache.spark.ml.pipes


import java.io.File

import data.dataset.DatasetLoader
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


/**
  * Created by wolf on 29.04.2016.
  */
class ColumnRemover(override val uid: String) extends Transformer
  with HasInputCol with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("colremover"))

  def setInputCol(value: String): this.type = {
    set(inputCol, value)
  }


  override def transform(dataset: DataFrame): DataFrame = {
    println("Removing column : "+$(inputCol))
    dataset.drop($(inputCol))
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val fields = schema.filter(field => {
      !field.name.equals($(inputCol))
    })
    StructType(fields)
  }
}

class ColumnsRemover(override val uid: String) extends Transformer
  with HasInputCols with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("colsremover"))

  def setInputCol(value: Array[String]): this.type = {
    set(inputCols, value)
  }


  override def transform(dataset: DataFrame): DataFrame = {
    var df = dataset

    $(inputCols).foreach(colName => {
      df = dataset.drop(colName)
    })

    df
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val cols = $(inputCols)
    val fields = schema.filter(field => {
      !cols.contains(field.name)
    })
    StructType(fields)
  }
}


class ColumnRenamer(override val uid: String, dropIfNecessary: Boolean) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this(dropIfNecessary: Boolean = false) = this(Identifiable.randomUID("colrename"), dropIfNecessary)

  def setInputCol(value: String): this.type = {
    set(inputCol, value)
  }

  def setOutputCol(value: String): this.type = {
    set(outputCol, value)
  }


  override def transform(dataset: DataFrame): DataFrame = {
    if (dropIfNecessary && dataset.columns.contains($(outputCol))) {
      dataset.drop($(outputCol)).withColumnRenamed($(inputCol), $(outputCol))
    }
    else if (dataset.columns.contains($(inputCol))) {
      dataset.withColumnRenamed($(inputCol), $(outputCol))
    }
    else {
      dataset
    }
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val fieldnames = schema.fields.map(field => field.name)
    if (fieldnames.contains($(inputCol)) && !fieldnames.contains($(outputCol))) {
      val fields = schema.map(field => {
        if (field.name.equals($(inputCol)))
          StructField($(outputCol), field.dataType, field.nullable, field.metadata)
        else
          field
      })
      StructType(fields)
    }
    else {
      schema
    }
  }
}

class DFSinker(override val uid: String) extends Estimator[DFSinkerModel] with HasInputCols with HasLabelCol {
  def this() = this(Identifiable.randomUID("dfsinker"))

  val trainFilename = new Param[String](this, "train filename", "dataframe training  filename")
  val testFilename = new Param[String](this, "test filename", "dataframe testing filename")

  def setInputCols(values: Array[String]): this.type = {
    set(inputCols, values)
  }

  def setLabelCol(value: String): this.type = {
    set(labelCol, value)
  }

  def setTrainFilename(value: String): this.type = {
    set(trainFilename, value)
  }

  def setTestFilename(value: String): this.type = {
    set(testFilename, value)
  }

  override def fit(dataset: DataFrame): DFSinkerModel = {

    val trainFolder = $(trainFilename)
    if (!DatasetLoader.exists(trainFolder)) {
      val cols = $(inputCols) :+ $(labelCol)
      val df: DataFrame = dataset.select(cols.head, cols.tail: _*)
      df.write.save(trainFolder)
    }
    new DFSinkerModel(uid, $(testFilename))
      .setInputCols($(inputCols))
      .setLabelCol($(labelCol))
  }


  override def copy(extra: ParamMap): Estimator[DFSinkerModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}

class DFSinkerModel(override val uid: String, filename: String) extends Model[DFSinkerModel] with HasInputCols with HasLabelCol {
  override def copy(extra: ParamMap): DFSinkerModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {

    if (!DatasetLoader.exists(filename)) {
      val cols = $(inputCols) :+ $(labelCol)
      val df: DataFrame = dataset.select(cols.head, cols.tail: _*)
      df.write.save(filename)
    }
    dataset
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  def setInputCols(values: Array[String]): this.type = {
    set(inputCols, values)
  }

  def setLabelCol(value: String): this.type = {
    set(labelCol, value)
  }

}


object AttributeCreator {
  def create(array: Array[String]): Array[Attribute] = {
    val defaultAttr = NumericAttribute.defaultAttr
    /*map(attr => attr.replaceAll("\\s", "_")).*/
    array.map(defaultAttr.withName)
  }

  def createFiled(array: Array[String], outputCol: String): StructField = {
    val attrs = create(array)
    val attrGroup = new AttributeGroup(outputCol, attrs)
    attrGroup.toStructField()
  }



  def append(schema: StructType, outputCol: String, array: Array[String]): StructType = {
    val attrs = create(array)
    val attrGroup = new AttributeGroup(outputCol, attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

object FileProcess {
  def createFolder(filename: String): Boolean = {
    new File(filename).getParentFile().mkdirs()
  }
}










