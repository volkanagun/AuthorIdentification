package org.apache.spark.ml.pipelines


import options.ParamsEvaluation
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.pipes.Covariate
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by wolf on 20.06.2016.
  */
class PipelineCovariate(val trainDF: DataFrame, val testDF: DataFrame) {

  def this(dataFrame: Array[DataFrame]) = this(dataFrame(0), dataFrame(1))

  val assembleColumns = new PipelineAssembler().numericColumns() ++ new PipelineAssembler().weightCols()

  val markedDF = trainDF.select(assembleColumns.head, assembleColumns.tail: _*).withColumn(PipelineCovariate.sourceLabelCol, lit(1.0))
    .unionAll(testDF.select(assembleColumns.head, assembleColumns.tail: _*).withColumn(PipelineCovariate.sourceLabelCol, lit(0.0)))

  val vectorAssembler = new VectorAssembler()
    .setInputCols(assembleColumns)
    .setOutputCol(PipelineAssembler.featuresCol)


  def measure(): Double = {
    if (ParamsEvaluation.covariateByClassification) byClassification()
    else if (ParamsEvaluation.covariateByKLDivergence) byKLDivergence()
    else byCorrelationDist()

  }


  protected def byClassification(): Double = {

    val testRepeat = 3

    val naiveBayes = new NaiveBayes()
      .setFeaturesCol(PipelineAssembler.featuresCol)
      .setLabelCol(PipelineCovariate.sourceLabelCol)
      .setPredictionCol(PipelineCovariate.sourcePredictionCol)


    val assembledDF = vectorAssembler.transform(markedDF)

    var averageTrues = 0.0
    for (i <- 0 until testRepeat) {
      val trainTestDF = assembledDF.randomSplit(ParamsEvaluation.trainTestSplit)
      val trainDF = trainTestDF(0)
      val testDF = trainTestDF(1)
      val model = naiveBayes.fit(trainDF)
      val newDF = model.transform(testDF)
      val fullCount = testDF.count()
      val trueCount = newDF.select(PipelineCovariate.sourcePredictionCol, PipelineCovariate.sourceLabelCol)
        .filter(newDF(PipelineCovariate.sourcePredictionCol) === newDF(PipelineCovariate.sourceLabelCol)).count().toDouble
      averageTrues += trueCount / fullCount
    }

    averageTrues / testRepeat

  }

  protected def byKLDivergence(): Double = {

    //build row matrix from features as Vector in rows of an RDD
    println("Measuring covariate by Kullback-Leibler Divergence")
    val trainRDD = vectorAssembler.transform(trainDF.select(assembleColumns.head, assembleColumns.tail: _*))
      .select(PipelineAssembler.featuresCol).map(row => {
      row.getAs[Vector](0)
    })

    val testRDD = vectorAssembler.transform(testDF.select(assembleColumns.head, assembleColumns.tail: _*))
      .select(PipelineAssembler.featuresCol).map(row => {
      row.getAs[Vector](0)
    })



    val trainMatrix: RowMatrix = new RowMatrix(trainRDD)
    val testMatrix: RowMatrix = new RowMatrix(testRDD)

    Covariate.KLMeasureOfCov(trainMatrix, testMatrix)
  }


  protected def byCorrelationDist(): Double = {

    //build row matrix from features as Vector in rows of an RDD
    println("Measuring covariate by Correlation Matrix Distance")
    val trainRDD = vectorAssembler.transform(trainDF.select(assembleColumns.head, assembleColumns.tail: _*))
      .select(PipelineAssembler.featuresCol).map(row => {
      row.getAs[Vector](0)
    })

    val testRDD = vectorAssembler.transform(testDF.select(assembleColumns.head, assembleColumns.tail: _*))
      .select(PipelineAssembler.featuresCol).map(row => {
      row.getAs[Vector](0)
    })



    val trainMatrix: RowMatrix = new RowMatrix(trainRDD)
    val testMatrix: RowMatrix = new RowMatrix(testRDD)

    Covariate.cmdMeasure(trainMatrix, testMatrix)
  }


}

object PipelineCovariate {
  val sourceLabelCol = "source-label"
  val sourcePredictionCol = "source-prediction"

}
