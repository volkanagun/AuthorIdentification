package org.apache.spark.ml.pipes

import java.io.PrintWriter

import cc.mallet.types.Multinomial.Estimator
import data.dataset.XMLParser
import options._
import options.Resources.Global
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Model, PipelineStage}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.pipelines.{PipelineClassification, PipelineLabelling}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType


import scala.util.Random

/**
  * Created by wolf on 03.05.2016.
  */
class EvaluatorPipe() extends Serializable {

  protected def restUnion(trainTest:Array[DataFrame], split:Array[DataFrame], k:Int): Array[DataFrame] ={
    for(i<-0 until split.length){
      if(i == k){
        trainTest(1) = trainTest(1).unionAll(split(i))
      }
      else{
        trainTest(0) = trainTest(0).unionAll(split(i))
      }
    }
    trainTest
  }

  protected def restUnion(trainTest:Array[DataFrame], split:Array[DataFrame], destination:DataFrame, k:Int): Array[DataFrame] ={
    for(i<-0 until split.length){
      if(i == k){
        trainTest(1) = trainTest(1).unionAll(destination)
      }
      else{
        trainTest(0) = trainTest(0).unionAll(split(i))
      }
    }
    trainTest
  }

  protected def crossLabels(df:DataFrame, labelCol:String, srcDstGenre:Array[String]):Array[String]={
    val sourceDF = df.filter(df(LoaderPipe.genreCol) === srcDstGenre(0))
    val destinationDF = df.filter(df(LoaderPipe.genreCol) === srcDstGenre(1))

    //Select authors from source and destination domain
    val countSourceDF = sourceDF
      .groupBy(labelCol)
      .count()
    val calcSourceSize = ParamsEvaluation.trainMinSize
    val labelSource = countSourceDF.filter(countSourceDF("count") >= calcSourceSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    val countDestinationDF = destinationDF
      .groupBy(labelCol)
      .count()

    val calcDestinationSize = ParamsEvaluation.testMinSize
    val labelDestination = countDestinationDF.filter(countDestinationDF("count") >= calcDestinationSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    labelSource.intersect(labelDestination)
  }

  def evaluate(dataFrame: DataFrame): EvaluationResult = {
    evaluate(dataFrame, -1.0)
  }

  def stratefiedKFoldCrossGenre(sqlContext:SQLContext, df:DataFrame, labelCol:String,
                                srcDstGenre:Array[String], kfold:Int):Array[Array[DataFrame]]={
    val sc = sqlContext.sparkContext
    println(s"Splitting dataframe as cross-genre stratefied cross validation (fold size: $kfold)...")

    //First take source domain and destination domain
    val sourceDF = df.filter(df(LoaderPipe.genreCol) === srcDstGenre(0))
    val destinationDF = df.filter(df(LoaderPipe.genreCol) === srcDstGenre(1))

    val calcSourceSize = ParamsEvaluation.trainMinSize
    val calcDestinationSize = ParamsEvaluation.testMinSize
    //Select authors from source and destination domain

    val labelIntersect = crossLabels(df, labelCol,srcDstGenre)
    val weights = Array.fill[Double](kfold)(1d/(kfold-1))

    val array = Array.fill[Array[DataFrame]](kfold)(
      Array.fill[DataFrame](2)(
        sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)))
    //Fold source and destination domain together
    //Fold source as k-1
    labelIntersect.foreach(label=>{
      val testDF = destinationDF.filter(destinationDF(labelCol) === label)
        .limit(calcDestinationSize)
      val split = sourceDF.filter(sourceDF(labelCol) === label)
        .limit(calcSourceSize).randomSplit(weights)

      for(i<-0 until kfold-1){
        array(i) = restUnion(array(i), split,testDF, i)
      }
    })

    array
  }

  def stratefiedKFold(sqlContext: SQLContext, df: DataFrame, labelCol: String, kfold: Int): Array[Array[DataFrame]] = {
    val sc = sqlContext.sparkContext
    println(s"Splitting dataframe as stratified cross validation (fold size: $kfold)...")
    val countdf = df
      .groupBy(labelCol)
      .count()
    val calcSize = ParamsEvaluation.trainMinSize

    val labels = countdf.filter(countdf("count") >= calcSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    println(s"Number of labels: ${labels.length}")

    val array = Array.fill[Array[DataFrame]](kfold)(
      Array.fill[DataFrame](2)(
        sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)))

    val weights = Array.fill[Double](kfold)(1d/kfold)

    labels.foreach(label=> {
      val split = df.filter(df(labelCol) === label)
        .limit(calcSize)
        .randomSplit(weights)

      for(i<-0 until kfold){
        array(i) = restUnion(array(i),split, i)
      }
    })

    array
  }

  def equalSizeLimit(sqlContext:SQLContext, df:DataFrame, labelCol: String):Array[DataFrame]={
    val sc = sqlContext.sparkContext

    println("Splitting dataframe class size-equally as train and test...")

    val calcSize = ParamsEvaluation.trainMinSize

    val countdf = df
      .groupBy(labelCol)
      .count()

    val labels = countdf.filter(countdf("count") >= calcSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    println("Selected label size : " + labels.length)

    val unionTrain = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val unionTest = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val trainTest = Array(unionTrain, unionTest)
    val ratio = ParamsEvaluation.trainTestSplit


    labels.foreach(label => {
      val split = df.filter(df(labelCol) === label)
        .randomSplit(ParamsEvaluation.trainTestSplit)

      split(0) = df.limit(ParamsEvaluation.trainMinSize)
      split(1) = split(1).limit(ParamsEvaluation.testMinSize)

      trainTest(0) = trainTest(0).unionAll(split(0))
      trainTest(1) = trainTest(1).unionAll(split(1))

      println("Constructed for " + label + " ...")

    })

    trainTest
  }

  def equalSizeSplit(sqlContext: SQLContext, df: DataFrame, labelCol: String): Array[DataFrame] = {
    val sc = sqlContext.sparkContext

    println("Splitting dataframe class size-equally as train and test...")

    val calcSize = ((ParamsEvaluation.trainMinSize + 1) / ParamsEvaluation.trainTestSplit(0)).toInt

    val countdf = df
      .groupBy(labelCol)
      .count()

    countdf.show(false)

    val labels = countdf.filter(countdf("count") > calcSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    println("Selected label size : " + labels.length)

    val unionTrain = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val unionTest = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val trainTest = Array(unionTrain, unionTest)
    val ratio = ParamsEvaluation.trainTestSplit

    /*val trainTest = df.filter( df(labelCol) === labels.head)
      .randomSplit(ParamsEvaluation.trainTestSplit)
    */

    labels.foreach(label => {
      val split = df.filter(df(labelCol) === label)
        .randomSplit(ParamsEvaluation.trainTestSplit)

      split(0) = split(0).limit(ParamsEvaluation.trainMinSize)
      split(1) = split(1).limit(ParamsEvaluation.testMinSize)

      trainTest(0) = trainTest(0).unionAll(split(0))
      trainTest(1) = trainTest(1).unionAll(split(1))

      println("Constructed for " + label + " ...")

    })
    trainTest
  }

  def equalSizeCrossGenre(sqlContext:SQLContext, df:DataFrame, labelCol:String, srcDstGenre:Array[String]):Array[DataFrame]={
    val sc = sqlContext.sparkContext
    println("Splitting data cross-genre class size equally...")
    val sourceDF = df.filter(df(LoaderPipe.genreCol)===srcDstGenre(0))
    val destinationDF = df.filter(df(LoaderPipe.genreCol)===srcDstGenre(1))
    val srcMinSize = ParamsEvaluation.trainMinSize
    val dstMinSize = ParamsEvaluation.testMinSize
    val labels = crossLabels(df,labelCol,srcDstGenre)
    //Union train and tests

    val trainTest = Array.fill[DataFrame](2)(sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema))

    labels.foreach(label=>{
      val trainDF = sourceDF.filter(sourceDF(labelCol) === label)
        .limit(srcMinSize)
      val testDF = destinationDF.filter(destinationDF(labelCol) === label)
        .limit(dstMinSize)
      trainTest(0) = trainTest(0).unionAll(trainDF)
      trainTest(1) = trainTest(1).unionAll(testDF)
    })

    trainTest

  }

  def equalPercentageSplit(sqlContext: SQLContext, df: DataFrame, labelCol: String): Array[DataFrame] = {

    val sc = sqlContext.sparkContext



    println("Splitting dataframe class equally as train and test...")

    val calcSize = ((ParamsEvaluation.trainMinSize + 1) / ParamsEvaluation.trainTestSplit(0)).toInt

    val countdf = df
      .groupBy(labelCol)
      .count()

    countdf.show(false)

    val labels = countdf.filter(countdf("count") > calcSize)
      .select(labelCol).map(row => row.getString(0)).collect()

    println("Selected label size : " + labels.length)

    val unionTrain = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val unionTest = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val trainTest = Array(unionTrain, unionTest)


    /*val trainTest = df.filter( df(labelCol) === labels.head)
      .randomSplit(ParamsEvaluation.trainTestSplit)
    */

    labels.foreach(label => {
      val split = df.filter(df(labelCol) === label)
        .randomSplit(ParamsEvaluation.trainTestSplit)

      trainTest(0) = trainTest(0).unionAll(split(0))
      trainTest(1) = trainTest(1).unionAll(split(1))

      println("Constructed for " + label + " ...")

    })



    /*trainTest(0).count()
    trainTest(1).count()*/

    trainTest
  }

  def evaluate(dataFrame: DataFrame, covariate: Double): EvaluationResult = {

    println("Evaluating predictions....")

    val evalResult = new EvaluationResult()

    /*val df = dataFrame.cache()*/


    println("Finding prediction and labels...")

    val predictionAndLabels = dataFrame.select(PipelineClassification.authorPrediction, PipelineLabelling.authorLabelCol)
      .map { case Row(prediction: Double, label: Double) =>
        (prediction, label)
      }

    //Checkpoint here
    predictionAndLabels.checkpoint()


    println("Finding map of labels...")
    val labelRDD = dataFrame.select(PipelineLabelling.authorLabelCol, LoaderPipe.authorCol)
      .map { case (Row(label: Double, name: String)) => (label, name) }

    //Check point here
    labelRDD.checkpoint()

    val labelMap = labelRDD.collectAsMap()

    println("Finding label counts...")
    val labelCounts = dataFrame.select(PipelineLabelling.authorLabelCol)
      .groupBy(PipelineLabelling.authorLabelCol).count().map {
      case Row(label: Double, count: Long) => (label, count)
    }.collectAsMap()


    println("Multiclass Metrics...")
    val multiclassMetrics = new MulticlassMetrics(predictionAndLabels)

    println("Finding measures...")
    evalResult.covariateMeasure = covariate
    evalResult.numTestInstances = predictionAndLabels.count()
    evalResult.numTestLabels = labelCounts.size.toLong

    evalResult.sampleRDDID = ParamsUnique.sampleRDDID()
    evalResult.sampleProcessedID = ParamsUnique.sampleProcessedID()
    evalResult.sampleEvalID = ParamsUnique.evalID()
    evalResult.classificationID = ParamsUnique.classificationID()
    evalResult.featuresID = ParamsUnique.featuresID()



    evalResult.fmeasure = multiclassMetrics.fMeasure
    evalResult.precision = multiclassMetrics.precision
    evalResult.recall = multiclassMetrics.recall
    evalResult.weightedFMeasure = multiclassMetrics.weightedFMeasure
    evalResult.weightedPrecision = multiclassMetrics.weightedPrecision
    evalResult.weightedRecall = multiclassMetrics.weightedRecall
    evalResult.weightedFalsePositive = multiclassMetrics.weightedFalsePositiveRate
    evalResult.weightedTruePositive = multiclassMetrics.weightedTruePositiveRate


    val labels = multiclassMetrics.labels

    labels.foreach(label => {

      val labelString = labelMap.getOrElse(label, label.toString)
      val labelCount = labelCounts.getOrElse(label, 0L)

      evalResult.setCount(labelString, labelCount)
      evalResult.setFMeasure(labelString, multiclassMetrics.fMeasure(label))
      evalResult.setPrecision(labelString, multiclassMetrics.precision(label))
      evalResult.setRecall(labelString, multiclassMetrics.recall(label))
      evalResult.setFalsePositive(labelString, multiclassMetrics.falsePositiveRate(label))
      evalResult.setTruePositive(labelString, multiclassMetrics.truePositiveRate(label))

    })

    if (ParamsEvaluation.printConfusion) {

      val confusionMatrix = multiclassMetrics.confusionMatrix

      confusionMatrix.foreachActive {
        case (i, j, value) => {
          val labeli = labels(i)
          val labelj = labels(j)
          val labeliname = labelMap.apply(labeli)
          val labeljname = labelMap.apply(labelj)
          evalResult.setConfusion(labeliname, labeljname, value)
        }
      }
    }

    //evalResult.writeAsXML()
    evalResult
  }


}

class EvaluationResult() {


  var covariateMeasure = 0.0

  var numTestInstances = 0L
  var numTestLabels = 0L

  var fmeasure = 0.0
  var precision = 0.0
  var recall = 0.0

  var weightedFMeasure = 0.0
  var weightedPrecision = 0.0
  var weightedRecall = 0.0

  var weightedFalsePositive = 0.0
  var weightedTruePositive = 0.0

  var labelCounts = Map[String, Double]()
  var labelFMeasures = Map[String, Double]()
  var labelPrecision = Map[String, Double]()
  var labelRecall = Map[String, Double]()
  var labelFalsePositives = Map[String, Double]()
  var labelTruePositives = Map[String, Double]()
  var confusionMatrix = Array[(String, String, Double)]()

  //Sample RDD id, sample DF id and processed DF id
  var sampleRDDID = -1
  var sampleEvalID = -1
  var sampleProcessedID = -1
  var classificationID = -1
  var featuresID = -1


  def addCount(label: String, count: Double): this.type = {
    val value = labelCounts.getOrElse(label, 0d) + count
    setCount(label, value.toLong)
  }

  def addFMeasure(label: String, count: Double): this.type = {
    val value = labelFMeasures.getOrElse(label, 0d) + count
    setFMeasure(label, value)
  }

  def addPrecision(label: String, count: Double): this.type = {
    val value = labelPrecision.getOrElse(label, 0d) + count
    setPrecision(label, value)
  }

  def addRecall(label: String, count: Double): this.type = {
    val value = labelRecall.getOrElse(label, 0d) + count
    setRecall(label, value)
  }

  def addFalsePositive(label: String, count: Double): this.type = {
    val value = labelFalsePositives.getOrElse(label, 0d) + count
    setFalsePositive(label, value)
  }

  def addTruePositive(label: String, count: Double): this.type = {
    val value = labelTruePositives.getOrElse(label, 0d) + count
    setTruePositive(label, value)
  }

  def addConfusion(labeli: String, labelj: String, count: Double): this.type = {
    val result = confusionMatrix.zipWithIndex.find(conf => {
      val pair = conf._1
      pair._1.equals(labeli) && pair._2.equals(labelj)
    })

    val value = result match {
      case Some(cnt) => {
        val index = cnt._2
        val pair = cnt._1
        ((pair._1, pair._2, pair._3+count),index)
      }
      case None => {
        ((labeli, labelj, count),-1)
      }
    }

    if(value._2 == -1){
      confusionMatrix = confusionMatrix :+ value._1
    }
    else{
      confusionMatrix = confusionMatrix.updated(value._2, value._1)
    }

    this
  }


  def setCount(label: String, count: Long): this.type = {
    labelCounts = labelCounts + (label -> count.toDouble)
    this
  }

  def setFMeasure(label: String, value: Double): this.type = {
    labelFMeasures = labelFMeasures + (label -> value.toDouble)
    this
  }

  def setPrecision(label: String, value: Double): this.type = {
    labelPrecision = labelPrecision + (label -> value.toDouble)
    this
  }

  def setRecall(label: String, value: Double): this.type = {
    labelRecall = labelRecall + (label -> value.toDouble)
    this
  }

  def setFalsePositive(label: String, value: Double): this.type = {
    labelFalsePositives = labelFalsePositives + (label -> value.toDouble)
    this
  }

  def setTruePositive(label: String, value: Double): this.type = {
    labelTruePositives = labelTruePositives + (label -> value.toDouble)
    this
  }


  /**
    *
    * @param labeli actual label
    * @param labelj predicted label
    * @param count
    */
  def setConfusion(labeli: String, labelj: String, count: Double): this.type = {
    confusionMatrix = confusionMatrix :+ (labeli, labelj, count)
    this
  }

  def add(evalResult: EvaluationResult): this.type = {
    this.sampleRDDID = evalResult.sampleRDDID
    this.sampleEvalID = evalResult.sampleEvalID
    this.sampleProcessedID = evalResult.sampleProcessedID
    this.classificationID = evalResult.classificationID
    this.featuresID = evalResult.featuresID

    val count = evalResult.numTestInstances
    val lbsize = evalResult.numTestLabels
    numTestInstances += count
    numTestLabels = lbsize
    covariateMeasure += count * evalResult.covariateMeasure
    fmeasure += count * evalResult.fmeasure
    precision += count * evalResult.precision
    recall += count * evalResult.recall
    weightedFMeasure += count * evalResult.weightedFMeasure
    weightedPrecision += count * evalResult.weightedPrecision
    weightedRecall += count * evalResult.weightedRecall
    weightedFalsePositive += count * evalResult.weightedFalsePositive
    weightedTruePositive += count * evalResult.weightedTruePositive

    val lbCount = evalResult.labelCounts
    val lbFMeasures = evalResult.labelFMeasures
    val lbPrecision = evalResult.labelPrecision
    val lbRecall = evalResult.labelRecall
    val lbTruePositives = evalResult.labelTruePositives
    val lbFalsePositives = evalResult.labelFalsePositives
    val lbConfusion = evalResult.confusionMatrix

    lbCount.foreach(pair => {
      addCount(pair._1, pair._2)
    })
    lbFMeasures.foreach(pair => {
      val value = lbCount.getOrElse(pair._1, 1d) * pair._2
      addFMeasure(pair._1, value)
    })
    lbPrecision.foreach(pair => {
      val value = lbCount.getOrElse(pair._1, 1d) * pair._2
      addPrecision(pair._1, value)
    })
    lbRecall.foreach(pair => {
      val value = lbCount.getOrElse(pair._1, 1d) * pair._2
      addRecall(pair._1, value)
    })
    lbTruePositives.foreach(pair => {
      val value = lbCount.getOrElse(pair._1, 1d) * pair._2
      addTruePositive(pair._1, value)
    })
    lbFalsePositives.foreach(pair => {
      val value = lbCount.getOrElse(pair._1, 1d) * pair._2
      addFalsePositive(pair._1, value)
    })
    lbConfusion.foreach(pair => addConfusion(pair._1, pair._2, pair._3))
    this
  }

  def finish(): this.type = {
    //average results by count and label count
    fmeasure = fmeasure / numTestInstances
    precision = precision / numTestInstances
    recall = recall / numTestInstances
    covariateMeasure = covariateMeasure / numTestInstances

    weightedFMeasure = weightedFMeasure / numTestInstances
    weightedPrecision = weightedPrecision / numTestInstances
    weightedRecall = weightedRecall / numTestInstances
    weightedTruePositive = weightedTruePositive / numTestInstances
    weightedFalsePositive = weightedFalsePositive / numTestInstances

    labelCounts.foreach {
      case (label, count) => {
        labelFMeasures = labelFMeasures + (label -> labelFMeasures.getOrElse(label, 0d) / count)
        labelPrecision = labelPrecision + (label -> labelPrecision.getOrElse(label, 0d) / count)
        labelRecall = labelRecall + (label -> labelRecall.getOrElse(label, 0d) / count)
        labelTruePositives = labelTruePositives + (label -> labelTruePositives.getOrElse(label, 0d) / count)
        labelFalsePositives = labelFalsePositives + (label -> labelFalsePositives.getOrElse(label, 0d) / count)
      }
    }


    this

  }

  def printMinResult(): Unit ={
    println("F-Measure: "+fmeasure)
    println("Precision: "+precision)
    println("Recall: "+recall)
  }

  def readPartialAsXML(filename: String): EvaluationResult = {
    val map = XMLParser.parseEvaluations(filename)
    val evalResult = new EvaluationResult()

    evalResult.numTestInstances = map.getOrElse("INSTANCESIZE", 0L.toString).toLong
    evalResult.numTestLabels = map.getOrElse("LABELSIZE", 0L.toString).toLong
    evalResult.covariateMeasure = map.getOrElse("COVARIATE", 0.0.toString).toDouble

    evalResult.sampleRDDID = map.getOrElse("RDD-ID", 0.toString).toInt
    evalResult.sampleEvalID = map.getOrElse("EVAL-ID", 0.toString).toInt
    evalResult.sampleProcessedID = map.getOrElse("PROCESSED-ID", 0.toString).toInt
    evalResult.classificationID = map.getOrElse("CLASSIFICATION-ID", 0.toString).toInt
    evalResult.featuresID = map.getOrElse("FEATURES-ID", 0.toString).toInt


    evalResult.fmeasure = map.getOrElse("F-MEASURE", 0.0.toString).toDouble
    evalResult.precision = map.getOrElse("PRECISION", 0.0.toString).toDouble
    evalResult.recall = map.getOrElse("RECALL", 0.0.toString).toDouble
    evalResult.weightedFMeasure = map.getOrElse("WEIGHTED-FMEASURE", 0.0.toString).toDouble
    evalResult.weightedPrecision = map.getOrElse("WEIGHTED-PRECISION", 0.0.toString).toDouble
    evalResult.weightedRecall = map.getOrElse("WEIGHTED-RECALL", 0.0.toString).toDouble
    evalResult.weightedFalsePositive = map.getOrElse("WEIGHTED-FALSEPOSITIVE", 0.0.toString).toDouble
    evalResult.weightedTruePositive = map.getOrElse("WEIGHTED-TRUEPOSITIVE", 0.0.toString).toDouble


    evalResult

  }

  def writeAsXML(): Unit = {
    val rnd = Random.nextInt()
    val filename = Global.evaluations + rnd.toString + ".xml"
    val printer = new PrintWriter(filename) {
      write("<EVALUATIONS LABEL=\"" + rnd + "\">\n")
      write("<RESULTS>\n")

      write(tagAttribute("INSTANCESIZE", numTestInstances.toString))
      write(tagAttribute("LABELSIZE", numTestLabels.toString))
      write(tagAttribute("COVARIATE", covariateMeasure.toString))

      write(tagAttribute("RDD-ID", sampleRDDID.toString))
      write(tagAttribute("PROCESSED-ID", sampleProcessedID.toString))
      write(tagAttribute("EVAL-ID", sampleEvalID.toString))
      write(tagAttribute("FEATURES-ID", featuresID.toString))
      write(tagAttribute("CLASSIFICATION-ID", classificationID.toString))



      write(tagAttribute("F-MEASURE", fmeasure.toString))
      write(tagAttribute("PRECISION", precision.toString))
      write(tagAttribute("RECALL", recall.toString))
      write(tagAttribute("WEIGHTED-FMEASURE", weightedFMeasure.toString))
      write(tagAttribute("WEIGHTED-PRECISION", weightedPrecision.toString))
      write(tagAttribute("WEIGHTED-RECALL", weightedRecall.toString))
      write(tagAttribute("WEIGHTED-FALSEPOSITIVE", weightedFalsePositive.toString))
      write(tagAttribute("WEIGHTED-TRUEPOSITIVE", weightedTruePositive.toString))

      labelCounts.foreach { case (label: String, value: Double) => {
        write("<LABEL VALUE=\"" + label + "\">\n")
        write(tag("COUNT", value.toString))
        write(tag("FMEASURE", labelFMeasures.getOrElse(label, 0.0).toString))
        write(tag("PRECISION", labelPrecision.getOrElse(label, 0.0).toString))
        write(tag("RECALL", labelRecall.getOrElse(label, 0.0).toString))
        write(tag("FALSEPOSITIVE", labelFalsePositives.getOrElse(label, 0.0).toString))
        write(tag("TRUEPOSITIVE", labelTruePositives.getOrElse(label, 0.0).toString))
        write("</LABEL>")
      }
      }

      write("<CONFUSION>\n")
      confusionMatrix.sorted.foreach(pair => {
        write("<LABEL ACTUAL=\"" + pair._1 + "\" PREDICTED=\"" + pair._2 + "\">\n")
        write(pair._3.toString)
        write("</LABEL>\n")
      })
      write("</CONFUSION>\n")
      write("</RESULTS>\n")


    }

    printer.write("<PARAMETERS>\n")

    ParamsWriter.appendAsXML(printer)

    printer.write("</PARAMETERS>\n")
    printer.write("</EVALUATIONS>")
    printer.close()
  }


  def tag(tagLabel: String, tagValue: String): String = {
    "<" + tagLabel + ">\n" + tagValue + "\n</" + tagLabel + ">\n"
  }

  def tagAttribute(tagLabel: String, tagValue: String): String = {
    "<MEASURE ATTR=\"" + tagLabel + "\">\n" + tagValue + "\n</MEASURE>\n"
  }

}





