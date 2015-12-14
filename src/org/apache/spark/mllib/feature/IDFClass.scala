package org.apache.spark.mllib.feature

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.IDFClass.{DocumentFrequencyAggregator}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 08.12.2015.
  */


trait IDFWeightingSchema extends Serializable {
  var termNormalization: Boolean = false
  var weightNormalization: Boolean = false

  def setTermNormalization(value: Boolean): IDFWeightingSchema = {
    termNormalization = value
    this
  }

  def setWeightNormalization(value: Boolean): IDFWeightingSchema = {
    weightNormalization = value
    this
  }


  def compute(a: Double, b: Double, c: Double, d: Double): Double
}








@Since("1.1.0")
@Experimental
class IDFClass(val weightingModel: IDFWeightingSchema, val labels:Array[Double], val termSize:Int) {

  /**
    * Computes the inverse document frequency.
    * @param dataset an RDD of term frequency vectors
    */
  @Since("1.1.0")
  def fit(dataset: RDD[(Vector, Double)]): IDFClassModel = {
    val idf = dataset.treeAggregate(new IDFClass.DocumentFrequencyAggregator(labels))(
      seqOp = (df, pair) => df.add(pair),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()
    new IDFClassModel(weightingModel, labels, termSize, idf)
  }


  /**
    * Computes the inverse document frequency.
    * @param dataset a JavaRDD of term frequency vectors
    */
  @Since("1.1.0")
  def fit(dataset: JavaRDD[Tuple2[Vector, Double]], labels: JavaRDD[Double]): Vector = {
    fit(dataset.rdd, labels.rdd)
  }
}

@Experimental
@Since("1.1.0")
class IDFClassModel private[spark](val weightingModel: IDFWeightingSchema,
                                   val labels: Array[Double],
                                   val termSize:Int,
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
    dataset.mapPartitions(iter => iter.map(v => IDFClassModel.transform(weightingModel, labels, bcIdf.value, v)))
  }

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector
    *
    * @param v a term frequency vector
    * @return a TF-IDF vector
    */
  @Since("1.3.0")
  def transform(v: Vector): Vector = IDFClassModel.transform(weightingModel, labels, idf, v)

  /**
    * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
    * @param dataset a JavaRDD of term frequency vectors
    * @return a JavaRDD of TF-IDF vectors
    */
  @Since("1.1.0")
  def transform(dataset: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(dataset.rdd).toJavaRDD()
  }
}

private object IDFClassModel {


  /**
    * Transforms a term frequency (TF) vector to a TF-IDF weighting vector with a selected IDF vector
    * Need review
    *
    * @param idfClass an IDF vector
    * @param v a term frequence vector
    * @return a TF-IDF vector
    */
  def transform(weightingModel: IDFWeightingSchema, labels: Array[Double], idfClass: Vector, v: Vector): Vector = {
    val n = v.size
    val nclass = labels.length * n

    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size
        val newValues = new Array[Double](nnz * labels.length)
        val newIndices = new Array[Int](nnz * labels.length)
        var mainIndex = 0

        for (cc <- 0 until labels.length) {
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
              newValues(mainIndex) = tcount / maxTerm * weight
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
              newValues(mainIndex) = tcount * weight / weightSum
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


        }

        Vectors.sparse(nclass, newIndices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n * labels.length)

        for (cc <- 0 until labels.length) {
          var j = 0
          val cindex = cc * n * 4
          val weights = Array.fill[Double](n)(0)
          var weightSum = 0d
          var maxTerm = 0d
          while (j < n) {
            val tcount = values(j)
            val a = idfClass(cindex + 0 * n + j)
            val b = idfClass(cindex + 1 * n + j)
            val c = idfClass(cindex + 2 * n + j)
            val d = idfClass(cindex + 3 * n + j)
            val weight = weightingModel.compute(a, b, c, d)

            weightSum += weight
            if (tcount > maxTerm) maxTerm = tcount
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
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

}

private object IDFClass {

  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator(val labels: Array[Double]) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Double] = _


    /** Adds a new document. */
    def add(pair: (Vector, Double)): this.type = {

      val doc = pair._1
      val labelValue = pair._2

      val featureSize = doc.size
      val labelSize = labels.length
      val fullSize = 4 * labelSize * featureSize

      if (isEmpty) {
        df = BDV.zeros(fullSize)
      }

      doc match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.size
          var k = 0
          while (k < nnz) {
            val fi = indices(k)

            for (li <- 0 until labelSize) {
              var ii = li * 4 * featureSize;
              if (values(k) > 0) {

                if (labelValue == labels(li)) {
                  //A value of k index
                  ii += fi
                }
                else {
                  //B value of k index
                  ii += featureSize + fi
                }


              }
              else {
                if (labelValue == labels(li)) {
                  //C value of k index
                  ii += 2 * featureSize + fi
                }
                else {
                  //D value of k index
                  ii += 3 * featureSize + fi
                }

              }

              df(ii) += 1L

            }

            k += 1
          }

        case DenseVector(values) =>
          val n = values.size
          var j = 0
          while (j < n) {
            val fi = j
            for (li <- 0 until labelSize) {
              var ii = li * 4 * featureSize
              if (values(j) > 0) {
                if (labelValue == labels(li)) {
                  //A value of k index
                  ii += fi
                }
                else {
                  //B value of k index
                  ii += featureSize + fi
                }
              }
              else {
                if (labelValue == labels(li)) {
                  //C value of k index
                  ii += 2 * featureSize + fi
                }
                else {
                  //D value of k index
                  ii += 3 * featureSize + fi
                }

              }

              df(ii) += 1L
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
    def merge(other: DocumentFrequencyAggregator): this.type = {
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


