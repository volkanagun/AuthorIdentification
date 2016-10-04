package org.apache.spark.ml

import options.{ParamsClassification, ParamsEvaluation}
import org.apache.spark.ml.pipelines.PipelineCovariate
import org.apache.spark.ml.pipes.{EvaluationResult, EvaluatorPipe, LoaderPipe}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by wolf on 16.09.2016.
  */

class MainExperimenter(sql: SQLContext) extends Serializable {

  //Repeat experiments 10 times weighted average results.
  val loaderPipe = new LoaderPipe()
  val evaluator = new EvaluatorPipe()
  val dfmain = loaderPipe.loadFlatDF(sql)

  var pipeline: Pipeline = null
  var kfold: Int = 10
  var isStratefied = false
  var isRandom = false
  var isCrossGenre = false
  var srcDstGenre = Array[String]()

  def setIsCrossGenre(value:Boolean):this.type ={
    isCrossGenre = value
    this
  }

  def setSrcDstGenre(srcDst:Array[String]):this.type ={
    srcDstGenre = srcDst
    this
  }

  def setIsStratefied(value:Boolean):this.type ={
    isStratefied = value
    this
  }
  def setIsRandom(value:Boolean):this.type ={
    isRandom = value
    this
  }

  def setPipeline(pipeline: Pipeline): this.type = {
    this.pipeline = pipeline
    this
  }

  def setKFold(kfold: Int): this.type = {
    this.kfold = kfold
    this
  }

  protected def modeling(trainTest: Array[DataFrame]): PipelineModel = {
    if (ParamsClassification.svmTransductiveAuthor) {
      pipeline.fit(trainTest(0).unionAll(trainTest(1)))
    }
    else {
      pipeline.fit(trainTest(0))
    }
  }

  protected def evaluating(trainTest: Array[DataFrame]): EvaluationResult = {
    val model = modeling(trainTest)
    val result = model.transform(trainTest(1))
    val evaluation = if (ParamsEvaluation.measureCovariate) {
      val covariate = new PipelineCovariate(trainTest).measure()
      evaluator.evaluate(result, covariate)
    }
    else {
      evaluator.evaluate(result)
    }

    evaluation.printMinResult()
    evaluation
  }

  protected def experimentOne(): Unit ={

    val trainTest = evaluator.equalSizeSplit(sql, dfmain, LoaderPipe.authorCol)
    val evalResult = evaluating(trainTest)

    evalResult.writeAsXML()
  }



  protected def experimentRandom(): Unit = {

    val evaluationResult = new EvaluationResult()

    for (i <- 0 until kfold) {
      val trainTest = evaluator.equalSizeSplit(sql, dfmain, LoaderPipe.authorCol)
      val evalResult = evaluating(trainTest)
      evaluationResult.add(evalResult)
    }

    evaluationResult
      .finish()
      .writeAsXML()
  }

  protected def experimentCrossRandom(): Unit = {

    val evaluationResult = new EvaluationResult()

    for (i <- 0 until kfold) {
      val trainTest = evaluator.equalSizeCrossGenre(sql, dfmain, LoaderPipe.authorCol, srcDstGenre)
      val evalResult = evaluating(trainTest)
      evaluationResult.add(evalResult)
    }

    evaluationResult
      .finish()
      .writeAsXML()
  }

  protected def experimentStratified(): Unit = {
    val evaluationResult = new EvaluationResult()
    val trainTestArray = evaluator.stratefiedKFold(sql, dfmain, LoaderPipe.authorCol, kfold)
    for (i <- 0 until kfold) {
      println("Experimenting on fold: "+(i+1))
      val trainTest = trainTestArray(i)
      //trainTest(0).printSchema()
      val evalResult = evaluating(trainTest)
      evaluationResult.add(evalResult)
    }

    evaluationResult
      .finish()
      .writeAsXML()
  }

  protected def experimentCrossStratified(): Unit = {
    val evaluationResult = new EvaluationResult()
    val trainTestArray = evaluator.stratefiedKFoldCrossGenre(sql, dfmain, LoaderPipe.authorCol, srcDstGenre, kfold)
    for (i <- 0 until kfold) {
      println("Experimenting on fold: "+(i+1))
      val trainTest = trainTestArray(i)
      val evalResult = evaluating(trainTest)
      evaluationResult.add(evalResult)
    }

    evaluationResult
      .finish()
      .writeAsXML()
  }

  def experiment(): Unit = {
    if(isCrossGenre && isRandom){
      experimentCrossRandom()
    }
    else if(isCrossGenre && isStratefied){
      experimentCrossStratified()
    }
    else if (isRandom) {
      experimentRandom()
    }
    else if (isStratefied) {
      experimentStratified()
    }
    else{
      experimentOne()
    }
  }

}
