package learning

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.classification.{RandomForestClassifier, NaiveBayes, DecisionTreeClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import processing.RDDProcessing
import structures.summary.PANDoc
import util.PrintBuffer

/**
 * Created by wolf on 31.10.2015.
 */
class PANPipeline {

  def cluster(): SparkContext = {
    val sparkHome: String = "/home/wolf/Documents/apps/spark-1.5.1-bin-hadoop2.6/"
    val conf = new SparkConf()
      .setAppName("author-identification")
      //.setMaster("local[48]")
      .setMaster("spark://192.168.1.3:7077")
      .setSparkHome(sparkHome)
      .set("spark.driver.memory", "48g")
      .set("spark.executor.memory", "2g")
      .set("spark.rdd.compress", "true")
      .setJars(Array("out/AuthorIdentification.jar"))
      .set("log4j.rootCategory", "INFO");

    val sc = new SparkContext(conf);
    return sc;
  }

  def local(): SparkContext = {

    val conf = new SparkConf()
      .setAppName("author-identification")
      .setMaster("local[8]")
      .set("spark.driver.memory", "72g")
      .set("spark.executor.memory", "8g")
      .set("spark.rdd.compress", "true")
      .set("log4j.rootCategory", "INFO");

    val sc = new SparkContext(conf);
    return sc;
  }

  def pipeline(sc: SparkContext, printBuffer: PrintBuffer): Unit = {

    val sqlContext: SQLContext = new SQLContext(sc)
    val processing: RDDProcessing = new RDDProcessing()
    val rndSplit: RandomSplit = new RandomSplit("label", Array[Double](0.9, 0.1))
    val panRDD: RDD[PANDoc] = processing.panRDD(sc)
    val df = sqlContext.createDataFrame(panRDD, classOf[PANDoc])

    val trainFrame = df.filter(df("docid").startsWith("LargeTrain"))
    //trainFrame.write.save("binary/pan-large-train")
    //val trainFrame = sqlContext.read.load("binary/large-train")
    val testFrame = df.filter(df("docid").startsWith("LargeTest"))
    //testFrame.write.save("binary/pan-large-test")
    //val testFrame = sqlContext.read.load("binary/large-test")

    val sentenceDetector = new OpenSentenceDetector()
      .setInputCol("text")
      .setOutputCol("sentences")

    val tokenizer = new OpenTokenizer()
      .setInputCol(sentenceDetector.getOutputCol)
      .setOutputCol("tokens")

    val posser = new OpenPOSTagger()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("pos-tags")

    //Goes to hashing TF to extract features
    val emoticondetector = new EmoticonDetector()
      .setInputCol(sentenceDetector.getOutputCol)
      .setOutputCol("emoticons")

    //Goes to hasing TF to extract features
    val ngramChars = new NGramChars()
      .setInputCol(sentenceDetector.getOutputCol)
      .setOutputCol("ngram-chars")
      .setMax(8)
      .setMin(2)


    //Goes to hasing TF to extract features
    val ngramWords = new NGramWords()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("ngram-words")
      .setMax(4)
      .setMin(2)

    //Goes to hasing TF to extract features
    val ngramPos = new NGramPOS()
      .setInputCol(posser.getOutputCol)
      .setOutputCol("ngram-pos")
      .setMax(4)
      .setMin(2)

    //Goes to hashing TF to extract features
    val repeatedPuncs = new RepeatedPuncs()
      .setInputCol(sentenceDetector.getOutputCol)
      .setOutputCol("repeated-puncs")


    //Already features in Vector
    val wordForms = new WordForms()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("word-form-features")

    //Already features in Vector
    val wordLengths = new WordLengthCount()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("word-length-features")
      .setMaxWordLength(50).setMinWordLength(1)

    //Already features in Vector
    val countFeatures = new LengthTokenLevel()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("count-features")


    /** ***
      * Hashing TF's for tokens, emoticons,ngram-chars, ngram-words
      */
    val tokenHashingTF = new ModifiedTFIDF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("token-raw-features")

    val tokenIDF = new IDF().setMinDocFreq(2)
      .setInputCol("token-raw-features").setOutputCol("token-features")


    val posHashingTF = new HashingTF().setNumFeatures(1000)
      .setInputCol(posser.getOutputCol)
      .setOutputCol("pos-features")

    val emoHashingTF = new ModifiedTFIDF()
      .setNumFeatures(1000).setInputCol(emoticondetector.getOutputCol)
      .setOutputCol("emo-features")

    val ngramCharHashingTF = new ModifiedTFIDF()
      .setNumFeatures(1000).setInputCol(ngramChars.getOutputCol)
      .setOutputCol("ngram-chars-features")

    val ngramWordHashingTF = new ModifiedTFIDF()
      .setNumFeatures(1000).setInputCol(ngramWords.getOutputCol)
      .setOutputCol("ngram-words-features")


    val ngramPosHashingTF = new HashingTF()
      .setNumFeatures(1000).setInputCol(ngramPos.getOutputCol)
      .setOutputCol("ngram-pos-features")


    val puncsHashingTF = new ModifiedTFIDF()
      .setNumFeatures(1000).setInputCol(repeatedPuncs.getOutputCol)
      .setOutputCol("punc-features")

    val stringIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexed-label")

    val assembler = new VectorAssembler().setInputCols(
      Array(
        tokenIDF.getOutputCol,
        posHashingTF.getOutputCol,
        emoHashingTF.getOutputCol,
        ngramCharHashingTF.getOutputCol,
        ngramWordHashingTF.getOutputCol,
        ngramPosHashingTF.getOutputCol,
        puncsHashingTF.getOutputCol,
        wordForms.getOutputCol,
        wordLengths.getOutputCol,
        countFeatures.getOutputCol))
      .setOutputCol("features")

    val nvb: NaiveBayes = new NaiveBayes()
      .setLabelCol("label").setFeaturesCol("features")

    val decisionTree = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val randomForest = new RandomForestClassifier()
    randomForest.setLabelCol("indexed-label").setFeaturesCol("features")


    val pipeline = new Pipeline()
      .setStages(Array[PipelineStage](
        sentenceDetector, tokenizer,posser,
        emoticondetector,ngramChars,
        ngramWords,
        ngramPos,
        repeatedPuncs,
        wordForms,
        wordLengths,
        countFeatures,
        tokenHashingTF,
        tokenIDF,
        emoHashingTF,
        posHashingTF,
        ngramCharHashingTF,
        ngramWordHashingTF,
        ngramPosHashingTF,
        puncsHashingTF,
        assembler
        //,stringIndexer,randomForest
      ))


    val wekaSink = new WekaARFFSink
    val pipelineModel = pipeline.fit(trainFrame)
    val transformedTrain = pipelineModel.transform(trainFrame)
    val transformedTest = pipelineModel.transform(testFrame)
    val labels = wekaSink.sinkTrain("train.arff", transformedTrain)
    wekaSink.sinkTest("test.arff", labels, transformedTest)




    /*val paramGrid = new ParamGridBuilder()
      .addGrid(wordHashingTF.numFeatures, Array[Int](100, 1000, 10000))
      .addGrid(emoHashingTF.numFeatures, Array[Int](100, 1000, 10000))
      .addGrid(ngramCharHashingTF.numFeatures, Array[Int](100, 1000, 10000))
      .addGrid(ngramWordHashingTF.numFeatures, Array[Int](100, 1000, 10000))
      .addGrid(puncsHashingTF.numFeatures, Array[Int](100, 1000, 10000))
      .build()*/

    //transform vectors via pipeline
    /*var i = 0
    while (i < 10) {
      val trainTestDf: Array[DataFrame] = dtframe.randomSplit(Array[Double](0.9,0.1))
      val trainFrame: DataFrame = trainTestDf(0).cache
      val testFrame: DataFrame = trainTestDf(1).cache

      val pipelineModel = pipeline.fit(trainFrame)
      val predictions = pipelineModel.transform(testFrame)
      printBuffer.addLine(s"TEST NUMBER:$i")
      printResults(printBuffer, trainFrame, testFrame, predictions)
      printBuffer.print()
      i += 1
    }*/

    /* val crossval = new CrossValidator()
       .setEstimator(pipeline)
       .setEvaluator(new MulticlassClassificationEvaluator())



     crossval.setEstimatorParamMaps(paramGrid)
     crossval.setNumFolds(10)

     val model = crossval.fit(trainFrame)
     val predictions = model.transform(testFrame)
     */
    //predictions.write.save("predictions-1.table")
    //val predictions = sqlContext.read.load("prediction.table")
    //predictions.write.save("prediction.table")
    /*printResults(printBuffer, trainFrame, testFrame, predictions)
    printBuffer.print()*/


  }

  def printResults(buffer: PrintBuffer,
                   trainFrame: DataFrame,
                   testFrame: DataFrame,
                   predictionFrame: DataFrame): Unit = {

    val predLabels = predictionFrame.select("prediction", "indexed-label").map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predLabels)
    val trainCount: Long = trainFrame.count
    val testCount: Long = testFrame.count
    val fmeasure: Double = metrics.fMeasure
    val precision: Double = metrics.precision
    val recall: Double = metrics.recall
    val matrix: Matrix = metrics.confusionMatrix

    buffer.addLine("BOW Model with Decision Tree Classifier")
    buffer.addLine("Number of training instances: " + trainCount)
    buffer.addLine("Number of testing instances: " + testCount)
    buffer.addLine("Precision:" + precision)
    buffer.addLine("Recall:" + recall)
    buffer.addLine("F1-Measure:" + fmeasure)
    buffer.addLine("Confusion:" + matrix.toString())
    buffer.print()
  }

}

object Test {
  def main(args: Array[String]) {
    val pipeline = new PANPipeline()
    val buffer = new PrintBuffer
    pipeline.pipeline(pipeline.cluster(), buffer)

  }
}
