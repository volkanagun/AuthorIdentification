package processing.pipelines

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import processing.RDDProcessing
import processing.structures.summary.PANDoc
import util.PrintBuffer

/**
  * Created by wolf on 12.12.2015.
  */
object PipelineUtils {
  def local():SparkContext={
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

  def loadPANSmall(sc:SparkContext,docIDFilter:String, topDocFreq:Int):DataFrame={
    val sqlContext: SQLContext = new SQLContext(sc)
    val processing: RDDProcessing = new RDDProcessing()
    val panRDD: RDD[PANDoc] = processing.panSmallRDD(sc)
    val panGroupRDD:RDD[PANDoc] = processing.panRDDSortByDocFreq(panRDD,docIDFilter,topDocFreq);
    sqlContext.createDataFrame(panGroupRDD, classOf[PANDoc])
  }

  def labelCount(df: DataFrame, labelCol:String):Int= {
    df.select(labelCol).rdd.max()(new Ordering[Row]{
      override def compare(x: Row, y: Row): Int = {
        Ordering[Double].compare(x(0).asInstanceOf[Double],y(0).asInstanceOf[Double])
      }
    }).getAs[Double](0).toInt + 1
  }

  def printResults(trainFrame: DataFrame,
                   testFrame: DataFrame,
                   predictionFrame: DataFrame): Unit = {

    val buffer = new PrintBuffer
    val predLabels = predictionFrame.select("prediction", "label").map(row => (row.getDouble(0), row.getDouble(1)))

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
    buffer.addLine("Confusion:\n" + matrix.toString())
    buffer.print()
  }
}
