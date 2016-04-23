package org.apache.spark.mllib.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import processing.pipelines.PipelineUtils

/**
  * Created by wolf on 22.12.2015.
  */
object SVMTest {

  def originalSVM(sc:SparkContext): Unit ={
    var data = MLUtils.loadLibSVMFile(sc, "data/mllib/splice.txt")
    data = data.map(point=>{new LabeledPoint(if(point.label<0) 0d else 1d,point.features)})


    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 500
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def kernelizedSVM(sc:SparkContext): Unit =
  {
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/splice.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 500
    val kernel:KernelFunction = new RBFKernel(20)
    val model = SVMKernelizedWithSGD.train(kernel, training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def kernelizedNonLinearSVM(sc:SparkContext): Unit =
  {
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/splice.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 30
    val kernel:KernelFunction = new RBFKernel(20)
    val model = SVMNonLinearWithSGD.train(kernel,training,numIterations,0.05,0.0001,1)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def kernelizedNonLinearParSVM(sc:SparkContext): Unit =
  {
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/splice.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 500
    val kernel:KernelFunction = new RBFKernel(50)
    val model = SVMNonLinearWithSGD.trainParallel(kernel,training,numIterations,0.05,0.0001,0.2)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def kernelizedNonLinearPPackSVM(sc:SparkContext): Unit =
  {
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/splice.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val lambda = 0.001
    val numIterations = 1000
    val kernel:KernelFunction = new RBFKernel(100)
    val model = SVMPPackSGD.train(kernel,data,lambda,1, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def main(args: Array[String]) {
    val sc = PipelineUtils.local()
    //originalSVM(sc)
    //kernelizedSVM(sc)
    //kernelizedNonLinearSVM(sc)
    kernelizedNonLinearParSVM(sc)
    //kernelizedNonLinearPPackSVM(sc)
  }
}
