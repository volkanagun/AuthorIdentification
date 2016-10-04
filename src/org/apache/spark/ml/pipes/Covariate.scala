package org.apache.spark.ml.pipes


import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.{SparkConf, SparkContext, mllib}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 22.06.2016.
  */
object Covariate {
  //convert dataframe into RowMatrix
  //compute covariance in Matrix from rowMatrix
  //compute mean from RowMatrix
  //convert everything to Breeze Matrices and do computations

  /**
    * Smaller divergence means more similar.
    *
    * @param trainM
    * @param testM
    * @return
    */
  def KLMeasureOfCov(trainM: RowMatrix, testM: RowMatrix): Double = {

    val trainMu = toBreeze(trainM.computeColumnSummaryStatistics().mean)
    val testMu = toBreeze(testM.computeColumnSummaryStatistics().mean)
    val d = trainMu.length

    val trainCov = covariance(trainM, trainMu)
    val testCov = covariance(testM, testMu)

    val trainDet: Double = det(trainCov)
    val testDet: Double = det(testCov)
    val divideDet = (testDet / (trainDet + 1.0E-10)) + 1.0E-10

    val muDiff = trainMu - testMu
    val muDiffMatrix = muDiff.toDenseMatrix.t
    val testInv = pinv(testCov)
    val traceVal = trace(testInv * trainCov)
    val multip = ((muDiff.toDenseMatrix * testInv) * muDiffMatrix)

    Math.log(divideDet) - d + traceVal +
      multip.valueAt(0)
  }

  def cmdMeasure(trainM: RowMatrix, testM: RowMatrix): Double = {
    val trainCov = trainM.computeCovariance()
    val testCov = testM.computeCovariance()

    val trainCovDense = new DenseMatrix[Double](trainCov.numRows, trainCov.numCols, trainCov.toArray)
    val testCovDense = new DenseMatrix[Double](testCov.numRows, testCov.numCols, testCov.toArray)
    val trce = trace(trainCovDense * testCovDense)

    1/(1 - trce/(frobeniusNorm(trainCovDense) * frobeniusNorm(testCovDense)))
  }

  protected def toBreeze(m: mllib.linalg.Matrix): breeze.linalg.DenseMatrix[Double] = {
    new breeze.linalg.DenseMatrix[Double](m.numRows, m.numCols, m.toArray)
  }

  protected def toBreeze(v: mllib.linalg.Vector): breeze.linalg.DenseVector[Double] = {
    v.toBreeze.toDenseVector
  }

  protected def toBreeze(rowMatrix: RowMatrix): breeze.linalg.DenseMatrix[Double] = {
    val m = rowMatrix.numRows().toInt
    val n = rowMatrix.numCols().toInt
    val mat = breeze.linalg.DenseMatrix.zeros[Double](m, n)
    var i = 0
    rowMatrix.rows.collect().foreach { v =>
      v.toBreeze.activeIterator.foreach { case (j, v) =>
        mat(i, j) = v
      }
      i += 1
    }
    mat
  }



  def covariance(matrix: RowMatrix, mean: breeze.linalg.DenseVector[Double]): DenseMatrix[Double] = {
    val n = matrix.numRows()
    val m = matrix.numCols()
    val mvec = DenseVector.fill(m.toInt) {
      n.toDouble
    }
    val diffMatrix: RDD[breeze.linalg.DenseVector[Double]] = matrix.rows.map(rowVec =>
      (rowVec.toBreeze.toDenseVector - mean))

    val covMat = diffMatrix.map(diffVec => {
      var vec = Array[Double]()

      diffVec.foreachPair { case (i, a) => {
        diffVec.foreachPair { case (j, b) => {
          vec = vec :+ a * b / n
        }
        }
      }
      }

      new DenseVector[Double](vec)
    }).treeReduce((sum, crr) => sum + crr)

    new DenseMatrix[Double](m.toInt, m.toInt, covMat.data)
  }

  def vectorSquareSum(vector: Vector[Double]): Double = {
    vector.fold(0.0)((a, b) => a + b * b)
  }

  def vectorCovariance(vector: Vector[Double], nrows: Long): Vector[Double] = {
    val ncols = vector.length
    var array = Array[Double]()
    vector.foreachPair((i1,v1)=>{
      vector.foreachPair((i2,v2)=>{
        array = array :+ v1*v2/nrows
      })
    })

    Vectors.dense(array).toBreeze
  }


  def frobeniusNorm(rowMatrix: DenseMatrix[Double]): Double = {
    val squareSum = rowMatrix.toDenseVector
      .foldLeft[Double](0.0)((sum,e)=>{sum + e*e})

    math.sqrt(squareSum)
  }

  def initLowLocal(): SparkContext = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("log4j.rootCategory").setLevel(Level.FINEST)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf: SparkConf = new SparkConf()
      .setAppName("Stemming Parsing")
      .setMaster("local[28]")
      .set("spark.default.parallelism", "6")
      .set("spark.sql.codegen","true")
      .set("spark.sql.tungsten.enabled","true")

      //.set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.driver.memory", "11g")

    return new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val vec = Vectors.dense(Array(1.0,2.0,3.0))
    val value = vectorCovariance(vec.toBreeze,1)

    val sc = initLowLocal()
    val vec1RDD = RandomRDDs.exponentialVectorRDD(sc, 43.5,  100L, 300)
    val vec2RDD = RandomRDDs.logNormalVectorRDD(sc, -0.5, 1.0, 100L, 300)
    val trainM = new RowMatrix(vec1RDD)
    val testM = new RowMatrix(vec2RDD)

    var tx = System.nanoTime()
    println(cmdMeasure(trainM,testM))
    var t0 = System.nanoTime()
    println("Time in ns for row matrix:" + (t0 - tx).toString)

    /*println(trainM.computeCovariance().toString())
    var t1 = System.nanoTime()
    println("Time in ns for row matrix:" + (t1 - t0).toString)
    println(covariance(trainM, trainM.computeColumnSummaryStatistics().mean.toBreeze.toDenseVector).toString())
    var t2 = System.nanoTime()
    println("Time in ns for mine: " + (t2 - t1).toString)*/
    //println(KLMeasure(trainM,testM))
  }

}
