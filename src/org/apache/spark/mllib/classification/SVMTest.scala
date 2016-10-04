package org.apache.spark.mllib.classification

import org.apache.spark.SparkContext
import org.apache.spark.ml.MainExecutor
import org.apache.spark.mllib.MLUtils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint


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
    val numIterations = 1000
    val kernel:KernelFunction = new RBFKernel(100)
    val model = SVMNonLinearWithSGD.train(kernel,training,numIterations,0.05,0.001,1)

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
    val training = MLUtils.loadLibSVMFile(sc, "train-libsvm.txt").map(lp=> LabeledPoint(if(lp.label==1.0) 1.0 else -1.0, lp.features)).cache()
    val test = MLUtils.loadLibSVMFile(sc, "test-libsvm.txt").map(lp=> LabeledPoint(if(lp.label==1.0) 1.0 else -1.0, lp.features)).cache()


    // Run training algorithm to build the model
    val lambda = 0.001
    val numIterations = 5000
    val kernel:KernelFunction = new DotKernel
    val model = SVMPPackSGD.train(kernel,training,lambda,2, numIterations)

    // Clear the default threshold.
    //model.setThreshold(0.0)

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (if(score==1.0) 1.0 else -1.0, point.label)
    }.cache()

    val trues= scoreAndLabels.filter{case(prediction,label)=>{
      prediction == label
    }}.count().toDouble

    val tcount = scoreAndLabels.count()

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Test count = "+tcount)
    println("Accuracy = "+trues/tcount)
    println("Area under ROC = " + auROC)
  }

  def originalLinearBinarySVM(sc:SparkContext): Unit =
  {
    val training = MLUtils.loadLibSVMFile(sc, "train-libsvm.txt").cache()
    val test = MLUtils.loadLibSVMFile(sc, "test-libsvm.txt").cache()


    // Run training algorithm to build the model
    val lambda = 0.001
    val numIterations = 1000

    val model = SVMWithSGD.train(training,numIterations,lambda,1.0)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }.cache()

    val trues= scoreAndLabels.filter{case(prediction,label)=>{
      prediction == label
    }}.count().toDouble


    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Accuracy = "+trues/scoreAndLabels.count())
    println("Area under ROC = " + auROC)
  }



  def main(args: Array[String]) {
    val sc = new MainExecutor().sc
    //originalSVM(sc)
    //kernelizedSVM(sc)
    //kernelizedNonLinearSVM(sc)
    //kernelizedNonLinearParSVM(sc)
    kernelizedNonLinearPPackSVM(sc)
    //originalLinearBinarySVM(sc)
  }
}
