package org.apache.spark.ml

import java.text.SimpleDateFormat
import java.util.Calendar

import breeze.linalg.DenseVector
import data.dataset.DatasetPrepare
import options._
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.classification.{OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IndexToString}
import org.apache.spark.ml.pipelines._
import org.apache.spark.ml.pipes._
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by wolf on 26.04.2016.
  */
class MainExecutor {

  val sc: SparkContext = initLocal()

  sc.setCheckpointDir("/tmp/")



  val loaderPipe = new LoaderPipe()
  val pipelineFeatures = new PipelineFeatures().explain()
  val pipelineWeighting = new PipelineWeighting()
  val pipelineSelection = new PipelineSelection()
  val pipelineFrequencies = new PipelineFrequencies()
  val pipelineTopicModels = new PipelineTopicModels()
  val pipelineWord2Vec = new PipelineWord2Vec()
  val pipelineLabelling = new PipelineLabelling()
  val pipelineClassification = new PipelineClassification()
  val pipelineCombiner = new PipelineAssembler()
  val pipeEvaluator = new EvaluatorPipe()


  ParamsReader.readAsXML("/home/wolf/Documents/java-projects/AuthorIdentification/params.xml")
  ParamsUnique.checkConsistency()

  //initialize spark context
  def initLocal(): SparkContext = {
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf()
      .setAppName("Author Identification")
      .setMaster("local[30]")
      .set("spark.default.parallelism", "30")
      .set("spark.sql.codegen","true")
      .set("spark.sql.tungsten.enabled","true")
      .set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "18g")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.driver.memory", "64g")

    val spark = new SparkContext(conf)
    /*val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)*/
    spark
  }

  def initLowLocal(): SparkContext = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("log4j.rootCategory").setLevel(Level.FINEST)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf: SparkConf = new SparkConf()
      .setAppName("Author Identification")
      .setMaster("local[28]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.codegen","true")
      .set("spark.sql.tungsten.enabled","true")

      //.set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.maxResultSize", "6g")
      .set("spark.driver.memory", "12g")

    return new SparkContext(conf)
  }

  def initCluster(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val usrDir: String = System.getProperty("user.dir")
    val sparkHome: String = "/home/wolf/Documents/apps/spark-1.6.2-bin-hadoop2.6/"
    val jarFile = usrDir + "/target/AuthorIdentification-1.0-jar-with-dependencies.jar"
    val conf: SparkConf = new SparkConf()
      .setAppName("Author Identification")
      .setSparkHome(sparkHome)
      .setMaster("spark://192.168.1.2:7077")
      .setJars(Array[String](jarFile))
      .set("spark.default.parallelism", "4")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.max value", "2048")
      //.set("spark.kryoserializer.buffer.max.mb", "1024")
      //.set("spark.driver.memory", "4g")
      //.set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    //sc.addJar(jarFile)
    sc
  }

  def printDate(): Unit ={
    val today = Calendar.getInstance.getTime
    val minuteFormat = new SimpleDateFormat("dd:hh:mm")

    println("Now : "+minuteFormat.format(today))
  }

  def authorPipeline(): Pipeline = {

    val stagesPreLabeling = pipelineLabelling.preClassification()
    val stagesFeatures = pipelineFeatures.pipeline()
    val stagesFrequencies = pipelineFrequencies.pipeline()
    val stagesTopicModel = pipelineTopicModels.pipeline()
    val stagesWord2Vec = pipelineWord2Vec.pipeline()

    val stagesWeighting = pipelineWeighting.pipeline()
    val stagesSelection = pipelineSelection.pipeline()
    val stagesCombiner = pipelineCombiner.pipeline()
    val stagesClassification = pipelineClassification.pipeline()
    val stagesPostLabeling = pipelineLabelling.postClassification()

    val stages =  stagesPreLabeling ++ stagesFeatures ++ stagesFrequencies ++
      stagesTopicModel ++
      stagesWord2Vec ++
      stagesWeighting ++ stagesSelection ++ stagesCombiner ++ stagesClassification ++
      stagesPostLabeling

    new Pipeline()
      .setStages(stages)

  }

  def equalSplit(sql:SQLContext):Array[DataFrame]={

    val dfFlat = loaderPipe.loadFlatDF(sql)
    val df = pipeEvaluator.equalSizeSplit(sql, dfFlat, LoaderPipe.authorCol)
    //pipeEvaluator.equalPercentageSplit(sql, dfFlat, LoaderPipe.authorCol) //dfFlat.randomSplit(ParamsEvaluation.trainTestSplit)

    val trainDF = df(0)
    val testDF = df(1)
    Array(dfFlat, trainDF, testDF)
  }

  def experiment(pipeline:Pipeline): Unit ={
    val sql = new SQLContext(sc)

    val experimenter = new MainExperimenter(sql)

    experimenter.setPipeline(pipeline)
      .setKFold(ParamsEvaluation.kfold)
      .setIsRandom(ParamsEvaluation.random)
      .setIsStratefied(ParamsEvaluation.stratified)
      .setIsCrossGenre(ParamsEvaluation.crossGenre)
      .setSrcDstGenre(ParamsEvaluation.srcDstGenre)

    experimenter.experiment()

  }

  def trainTest(): Unit = {

    val sql = new SQLContext(sc)
    val pipeline = authorPipeline()


    if(ParamsClassification.createTypeLMText){
      val dfFlat = equalSplit(sql)(0)
      val transformer = new PipesSamplerLM()
        .setSampleCol(LoaderPipe.textCol)
        .setTypeCol(LoaderPipe.genreCol)

      transformer.transform(dfFlat)
    }
    else if(ParamsClassification.createLDAmodel){
      val dfSentences = loaderPipe.loadSentencesDF(sql, false)
      pipelineTopicModels.ldaTransform(dfSentences)
    }
    else if(ParamsClassification.createWord2Vec){
      val dfSentences = loaderPipe.loadFlatSentencesDF(sql, false)
      pipelineWord2Vec.fit(dfSentences)
    }
    else if(ParamsClassification.createHLDAmodel){

      val dfSentences = loaderPipe.loadSentencesDF(sql, false)

      val transformer = new SamplerHLDA()
        .setSampleCol(LoaderPipe.tokensCol)
        .setDocIDCol(LoaderPipe.docidCol)

      transformer.transform(dfSentences)
    }
    else if (ParamsClassification.createWekaDataset)
    {
      //TODO : Random size limit
      val dfArr = equalSplit(sql)
      val trainDF = dfArr(1)
      val testDF = dfArr(2)
      val model = pipeline.fit(trainDF)
      //model.transform(trainDF)
      model.transform(testDF)
    }
    else if (ParamsClassification.createLibSVMDataset) {
      val dfArr = equalSplit(sql)
      val trainDF = dfArr(1)
      val testDF = dfArr(2)
      val model = pipeline.fit(trainDF)
      model.transform(trainDF)
      model.transform(testDF)
    }
    else{
      experiment(pipeline)
    }
    /*else {
      val dfArr = equalSplit(sql)
      val trainDF = dfArr(1)
      val testDF = dfArr(2)

      val mainModel = if(ParamsClassification.svmTransductiveAuthor){
        pipeline.fit(trainDF.unionAll(testDF))
      }
      else{
        pipeline.fit(trainDF)
      }

      val testTransform = mainModel.transform(testDF)

      if (ParamsEvaluation.measureCovariate) {
        val trainTransform = mainModel.transform(trainDF)
        val covariateMeasure = new PipelineCovariate(trainTransform, testTransform)
          .measure()
        pipeEvaluator.evaluate(testTransform, covariateMeasure)

      }
      else {
        pipeEvaluator.evaluate(testTransform)
      }
    }*/

    //model.write.overwrite().save(PipelineResources.executorModel)
  }
}

object MainExecutor {

  val repeat = 1


  def main(args: Array[String]) {

    for(i<-0 until repeat) {
      //Init spark context
      val mainExecutor = new MainExecutor()
      mainExecutor.printDate()
      mainExecutor.trainTest()
      mainExecutor.sc.stop()
      System.gc()
      System.gc()
    }
  }
}