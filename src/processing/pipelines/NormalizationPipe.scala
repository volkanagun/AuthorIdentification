package processing.pipelines

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorEqually, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, NaiveBayes}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.extraction.{EmoticonDetector, ModifiedTFIDF, OpenSentenceDetector, OpenTokenizer}
import org.apache.spark.ml.feature.reweighting.{IDFClass, ProbLIU}


/**
  * Created by wolf on 11.12.2015.
  */
class NormalizationPipe {
  def bowNormalization(): Unit = {

    val sc = PipelineUtils.local()
    val df = PipelineUtils.loadPANSmall(sc, "SmallTrain", 50)
    val dfSplit = df.randomSplit(Array(0.9, 0.1))
    val labelCount = PipelineUtils.labelCount(df,"label")
    val train = dfSplit(0)
    val test = dfSplit(1)

    val sentenceDetector = new OpenSentenceDetector()
      .setInputCol("text")
      .setOutputCol("sentences")

    val tokenizer = new OpenTokenizer()
      .setInputCol(sentenceDetector.getOutputCol)
      .setOutputCol("tokens")

    val tokenTF = new ModifiedTFIDF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("token-features")

    val emoticons = new EmoticonDetector()
    emoticons.setInputCol(sentenceDetector.getOutputCol)
    emoticons.setOutputCol("emoticons")

    val emoticonTF = new ModifiedTFIDF()
    emoticonTF.setInputCol(emoticons.getOutputCol)
    //emoticonTF.setOutputCol("emoticon-features")
    emoticonTF.setOutputCol("features")

    val probliu = new ProbLIU()
      .setTermNormalization(false)
      .setWeightNormalization(false)

    val idf = new IDFClass()
      .setWeightingModel(probliu)
      .setInputCol(tokenTF.getOutputCol)
      .setOutputCol("features")
      .setLabelCol("label")

    val idftf = new IDF()
    .setInputCol(tokenTF.getOutputCol)
    .setOutputCol("features")
    .setMinDocFreq(2)

    val mlp = new MultilayerPerceptronClassifier()
      .setFeaturesCol(idf.getOutputCol)
      .setLabelCol("label")
      .setLayers(Array[Int](100,300,labelCount))
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    val nvb: NaiveBayes = new NaiveBayes()
      .setLabelCol("label").setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array[PipelineStage](
        sentenceDetector,
        tokenizer,tokenTF,
        //emoticons, emoticonTF,
        idftf,
        mlp))

    val paramGrid = new ParamGridBuilder()
      .addGrid(tokenTF.numFeatures, Array[Int](100, 1000, 10000)).build()

    val crossval = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new MulticlassClassificationEvaluator())

    crossval.setNumFolds(10)

    val model = crossval.fit(train)
    val predictions = model.transform(test)

    PipelineUtils.printResults(train, test, predictions)

  }
}

object NormalizationPipe {
  def main(args: Array[String]) {
    val pipe = new NormalizationPipe
    pipe.bowNormalization()

  }
}
