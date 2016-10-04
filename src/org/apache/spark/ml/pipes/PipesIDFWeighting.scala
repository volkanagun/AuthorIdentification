package org.apache.spark.ml.pipes

import breeze.linalg.{DenseVector => BDV}
import com.sun.xml.internal.fastinfoset.util.StringArray
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.param.shared.{HasInputCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by wolf on 08.12.2015.
  */


trait IDFWeightingSchema extends Serializable {
  var termNormalization: Boolean = false
  var weightNormalization: Boolean = false
  var classBased: Boolean = false

  def setTermNormalization(value: Boolean): IDFWeightingSchema = {
    termNormalization = value
    this
  }

  def setWeightNormalization(value: Boolean): IDFWeightingSchema = {
    weightNormalization = value
    this
  }

  def setClassBased(value: Boolean): IDFWeightingSchema = {
    classBased = value
    this
  }

  def compute(a: Double, b: Double, c: Double, d: Double): Double
}

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Weighting Methods/Schemas">

class TFIDF extends IDFWeightingSchema {
  classBased = false

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d
    Math.log(N / (a + c))
  }
}

class ProbLIU extends IDFWeightingSchema {
  classBased = true

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001
    Math.log((1 + (A / B) * (A / C)))
  }
}

class OddsRatio extends IDFWeightingSchema {
  classBased = true

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {

    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    Math.log(A * D / B * C)
  }
}

class InfoGain extends IDFWeightingSchema {
  classBased = true

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {

    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    val N = a + b + c + d
    a / N * Math.log((A * N) / ((A + B) * (A + C))) + C / N * Math.log(C * N / ((C + D) * (A + C)))
  }
}

class CorrCoefficient extends IDFWeightingSchema {
  classBased = true

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d

    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    Math.sqrt(N) * (A * D - B * C) / Math.sqrt((A + B) * (B + D) * (A + B) * (C + D))
  }
}

class ChiSquare extends IDFWeightingSchema {
  classBased = true

  override def compute(a: Double, b: Double, c: Double, d: Double): Double = {
    val N = a + b + c + d
    val A = a + 0.0001
    val B = b + 0.0001
    val C = c + 0.0001
    val D = d + 0.0001

    val nom = (A * D - B * C)
    N * (nom * nom) / ((A + C) * (B + D) * (A + B) * (C + D))
  }
}

//</editor-fold>
///////////////////////////////////////////////////////////


trait IDFClassBase extends Params with HasInputCol with HasLabelCol with HasOutputCol {

  /**
    * WeightingType parameter for different mechanisms
    */
  var weightingModel: IDFWeightingSchema = new TFIDF

  def getWeightingModel: IDFWeightingSchema = weightingModel

  def setWeightingModel(scheme: IDFWeightingSchema): this.type = {
    weightingModel = scheme
    return this
  }

  val attributes = new StringArrayParam(this, "attribute array", "attribute names for this model")

  def setAttributes(value: Array[String]): this.type = set(attributes, value)

  def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT, "Input column must be VectorUDT")
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType, "Label column must be double and sorted")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

 /* protected def validateAndTransformSchema(schema: StructType, labels: Array[Double], termSize: Int): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT, "Input column must be VectorUDT")
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType, "Label column must be double and sorted")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }*/

  def validateAndTransformSchema(schema: StructType, nlabels: Int, attributes: Array[String]): StructType = {
    val defaultAttr = NumericAttribute.defaultAttr
    val attrSeq = if (weightingModel.classBased) {
      var attrs = Array[Attribute]()
      for (i <- 0 until nlabels) {
        attrs = attrs ++ attributes.map(attr => attr + "_class" + i.toString).map(defaultAttr.withName)
      }

      attrs
    }
    else {
      attributes.map(defaultAttr.withName)
    }

    val attrGroup = new AttributeGroup($(outputCol), attrSeq.asInstanceOf[Array[Attribute]])
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  protected def extractAttributes(schema: StructType): Array[String] = {
    val metadata = AttributeGroup.fromStructField(schema($(inputCol)))
    metadata.attributes.get.map(attr => attr.name.get)

  }


}

@Since("1.1.0")
@Experimental
class IDFClass(val weightingModel: IDFWeightingSchema, val labels: Array[Double], val termSize: Int) {
  /**
    * Computes the inverse document frequency.
    *
    * @param dataset an RDD of term frequency vectors
    */
  @Since("1.1.0")
  def fit(dataset: RDD[(Vector, Double)]): IDFClassModel = {
    println("Building IDFClass Model...")
    val idf = if (weightingModel.classBased) {
      dataset.treeAggregate(new IDFClass.ClassBasedDFAggregator(labels))(
        seqOp = (df, pair) => df.add(pair),
        combOp = (df1, df2) => df1.merge(df2)
      ).idf()
    }
    else {
      dataset.treeAggregate(new IDFClass.IDFAggregator(labels))(
        seqOp = (df, pair) => df.add(pair),
        combOp = (df1, df2) => df1.merge(df2)
      ).idf()
    }

    println("Building IDFClass Model is finished...")

    new IDFClassModel(weightingModel, labels, termSize, idf)
  }


  /**
    * Computes the inverse document frequency.
    *
    * @param dataset a JavaRDD of term frequency vectors
    */
  @Since("1.1.0")
  def fit(dataset: JavaRDD[Tuple2[Vector, Double]], labels: JavaRDD[Double]): Vector = {
    fit(dataset.rdd, labels.rdd)
  }
}

@Experimental
@Since("1.1.0")
class IDFClassModel(val weightingModel: IDFWeightingSchema,
                    val labels: Array[Double],
                    val termSize: Int,
                    val idf: Vector) extends Serializable {


  /**
    * Transforms term frequency (TF) vectors to TF-IDF vectors.
    *
    * If `minDocFreq` was set for the IDF calculation,
    * the terms which occur in fewer than `minDocFreq`
    * documents will have an entry of 0.
    *
    * @param dataset an RDD of term frequency vectors
    * @return an RDD of TF-IDF vectors
    */
  @Since("1.1.0")
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf)
    if (weightingModel.classBased) {
      dataset.mapPartitions(iter => iter.map(v => IDFClassModel.transform(weightingModel, labels, bcIdf.value, v)))
    }
    else {
      dataset.mapPartitions(iter => iter.map(v => IDFClassModel.transformIDF(bcIdf.value, v)))
    }
  }

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector
    *
    * @param v a term frequency vector
    * @return a TF-IDF vector
    */
  @Since("1.3.0")
  def transform(v: Vector): Vector = if (weightingModel.classBased) {
    IDFClassModel.transform(weightingModel, labels, idf, v)
  } else {
    IDFClassModel.transformIDF(idf, v)
  }

  /**
    * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
    *
    * @param dataset a JavaRDD of term frequency vectors
    * @return a JavaRDD of TF-IDF vectors
    */
  @Since("1.1.0")
  def transform(dataset: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(dataset.rdd).toJavaRDD()
  }
}


//<editor-fold defaultstate="collapsed" desc="Operations">

object IDFClassModel {

  def transformIDF(idfVector: Vector, v: Vector): Vector = {
    v match {
      case SparseVector(size, indices, values) => {

        val n = indices.length
        var k = 0
        val newValues = new Array[Double](n)
        val newIndices = new Array[Int](n)

        while (k < n) {
          val j = indices(k)
          val freq = values(k)
          newValues(k) = freq * idfVector(j)
          newIndices(k) = j
          k += 1
        }

        Vectors.sparse(size, newIndices, newValues)
      }
      case DenseVector(values) => {
        val n = values.length
        var k = 0
        val newValues = new Array[Double](n)
        while (k < n) {
          val freq = values(k)
          newValues(k) = freq * idfVector(k)
          k += 1
        }

        Vectors.dense(newValues)
      }

      case other => {
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }
  }

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF weighting vector with a selected IDF vector
    * Need review
    *
    * @param idfClass an IDF vector
    * @param v        a term frequence vector
    * @return a = TF-IDF vector
    */
  def transform(weightingModel: IDFWeightingSchema, labels: Array[Double], idfClass: Vector, v: Vector): Vector = {
    val n = v.size
    val nclass = labels.length * n
    val labelList = (0 until labels.length)

    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size

        //if(weightingModel.classBased) {
        val newValues = new Array[Double](nnz * labels.length)
        val newIndices = new Array[Int](nnz * labels.length)
        var mainIndex = 0

        labelList.foreach(cc => {
          var k = 0

          val cindex = cc * size * 4
          val weights = Array.fill[Double](nnz)(0)
          var weightSum = 0d
          var maxTerm = 0d

          while (k < nnz) {
            val j = indices(k)
            val tcount = values(k)

            val a = idfClass(cindex + 0 * size + j)
            val b = idfClass(cindex + 1 * size + j)
            val c = idfClass(cindex + 2 * size + j)
            val d = idfClass(cindex + 3 * size + j)

            val weight = weightingModel.compute(a, b, c, d)
            weightSum += weight
            weights(k) = weight
            if (tcount > maxTerm) maxTerm = tcount

            k += 1
          }

          if (weightingModel.termNormalization && weightingModel.weightNormalization) {
            k = 0
            while (k < nnz) {
              val index = cc * n + indices(k)
              val tcount = values(k)
              val weight = weights(k)
              newValues(mainIndex) = tcount / maxTerm * weight / weightSum
              newIndices(mainIndex) = index
              k += 1
              mainIndex += 1
            }
          }
          else if (weightingModel.termNormalization) {
            k = 0
            while (k < nnz) {
              val index = cc * n + indices(k)
              val tcount = values(k)
              val weight = weights(k)
              newValues(mainIndex) = (tcount / maxTerm) * weight
              newIndices(mainIndex) = index
              k += 1
              mainIndex += 1
            }
          }
          else if (weightingModel.weightNormalization) {
            k = 0
            while (k < nnz) {
              val index = cc * n + indices(k)
              val tcount = values(k)
              val weight = weights(k)
              newValues(mainIndex) = tcount * (weight / weightSum)
              newIndices(mainIndex) = index
              k += 1
              mainIndex += 1
            }
          }
          else {
            k = 0
            while (k < nnz) {
              val index = cc * n + indices(k)
              val tcount = values(k)
              val weight = weights(k)
              newValues(mainIndex) = tcount * weight
              newIndices(mainIndex) = index
              k += 1
              mainIndex += 1
            }
          }
        })

        Vectors.sparse(nclass, newIndices, newValues)

      case DenseVector(values) =>
        val newValues = new Array[Double](n * labels.length)

        labelList.foreach(cc => {
          var j = 0
          val cindex = cc * n * 4
          val weights = Array.fill[Double](n)(0)
          var weightSum = 0d
          var maxTerm = 0d
          while (j < n) {
            val a = idfClass(cindex + 0 * n + j)
            val b = idfClass(cindex + 1 * n + j)
            val c = idfClass(cindex + 2 * n + j)
            val d = idfClass(cindex + 3 * n + j)
            val weight = weightingModel.compute(a, b, c, d)

            weightSum += weight
            weights(j) = weight
            j += 1
          }

          if (weightingModel.termNormalization && weightingModel.weightNormalization) {
            j = 0
            while (j < n) {
              val tcount = values(j)
              val index = cc * n + j
              newValues(index) = (tcount / maxTerm) * weights(j) / weightSum
              j += 1
            }
          }
          else if (weightingModel.weightNormalization) {
            j = 0
            while (j < n) {
              val tcount = values(j)
              val index = cc * n + j
              newValues(index) = tcount * weights(j) / weightSum
              j += 1
            }
          }
          else if (weightingModel.termNormalization) {
            j = 0
            while (j < n) {
              val tcount = values(j)
              val index = cc * n + j
              newValues(index) = (tcount / maxTerm) * weights(j)
              j += 1
            }
          }
          else {
            j = 0
            while (j < n) {
              val tcount = values(j)
              val index = cc * n + j
              newValues(index) = tcount * weights(j)
              j += 1
            }
          }
        })

        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

}

object IDFClass {

  /** Document frequency aggregator. */
  class ClassBasedDFAggregator(val labels: Array[Double]) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Double] = _


    /** Adds a new document. */
    def add(pair: (Vector, Double)): this.type = {

      val doc = pair._1
      val labelValue = pair._2

      val labelSize = labels.length



      val denseVector = doc.toDense
      val values = denseVector.values
      val n = values.size
      val fullSize = 4 * labelSize * n

      if (isEmpty) {
        df = BDV.zeros(fullSize)
      }



      val parLoop = (0 until labelSize).toList
      parLoop.foreach(cc => {
        var ii = cc * 4 * n
        var j = 0
        //val cindex = cc * n * 4
        //val a = idfClass(cindex + 0 * n + j)
        while (j < n) {


          val dfi = if (values(j) > 0) {
            if (labelValue == labels(cc)) {
              //A value of j index
              //Positive document frequency, i.e., number of documents in positive category containing term ti.
              ii + j
            }
            else {
              //C Value
              ii + 2 * n + j
            }
          }
          else {
            if (labelValue == labels(cc)) {
              //B Number of documents in positive category which do not contain term ti.
              ii + 1 * n + j
            }
            else {
              //D value of j index
              ii + 3 * n + j
            }

          }

          df(dfi) += 1L
          j += 1
        }


      })

      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: ClassBasedDFAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      Vectors.fromBreeze(df)
    }
  }

  class IDFAggregator(val labels: Array[Double]) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Double] = _


    /** Adds a new document. */
    def add(pair: (Vector, Double)): this.type = {
      val doc = pair._1

      if (isEmpty) {
        df = BDV.zeros(doc.size)
      }
      doc match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.size
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L
            }
            k += 1
          }
        case DenseVector(values) =>
          val n = values.size
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: IDFAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      Vectors.fromBreeze(df)
    }
  }

}

//</editor-fold>


//<editor-fold desc="Estimator and Transformer Model">


class IDFWeighterModel(override val uid: String,
                       idfModel: IDFClassModel)
  extends Model[IDFWeighterModel] with IDFClassBase {


  val nlabels = idfModel.labels.length

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = validateAndTransformSchema(dataset.schema, nlabels, $(attributes))
    val metadata = outputSchema($(outputCol)).metadata

    val idf = udf { vec: Vector => idfModel.transform(vec) }
    //create metadata for output
    val ndf = dataset.select(col("*"), idf(col($(inputCol))).as($(outputCol), metadata))
    ndf
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, nlabels, $(attributes))
  }

  override def copy(extra: ParamMap): IDFWeighterModel = {
    val copied = new IDFWeighterModel(uid, idfModel)
    copyValues(copied, extra).setParent(parent)
  }
}

class IDFWeighter(override val uid: String) extends Estimator[IDFWeighterModel] with IDFClassBase {

  def this() = this(Identifiable.randomUID("tfidfclass"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def copy(extra: ParamMap): IDFWeighter = defaultCopy(extra)


  override def fit(df: DataFrame): IDFWeighterModel = {
    import df.sqlContext.implicits._
    //Find distinct class labels means number of classes in sorted order
    //For each class label use hashing tf : count the number of occurrence of a term in each class
    //Find total number of terms |T|
    //Find total number of samples |N|

    //Find the number of documents of category c where term k appears at least once (A)
    //Find the number of documents that is not from category c and term k appears at least once (B)
    //Find the number of document of category c where term k doesn't appear (C)
    //Find the number of documents that is not from category c and term k doesn't appear (D)

    println("Building IDF Model for " + $(inputCol))

    val nattributes = extractAttributes(df.schema)
    val outputSchema = transformSchema(df.schema, logging = true)
    val labelRDD = df.select($(labelCol)).distinct()
    val labels = labelRDD.map {
      case Row(l: Double) => l
    }.collect().sorted
    val input = df.select($(inputCol), $(labelCol)).map { case Row(v: Vector, l: Double) => (v, l) }
    val termSize = input.first()._1.size
    val idfc = new IDFClass(weightingModel, labels, termSize).fit(input)
    val weighterModel = new IDFWeighterModel(uid, idfc)
      .setParent(this)
      .setAttributes(nattributes)

    copyValues(weighterModel)


  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


}

//</editor-fold>




