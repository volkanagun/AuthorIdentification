package learning

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import processing.RDDProcessing
import structures.summary.PANDoc
import util.PrintBuffer

/**
 * Created by wolf on 31.10.2015.
 */
class PANPipeline {

  def local(): SQLContext = {
    val conf = new SparkConf().setAppName("test").setMaster("local[12]");
    val sc = new SparkContext(conf);
    return new SQLContext(sc);
  }

  def pipeline(sc: JavaSparkContext, printBuffer: PrintBuffer): Unit ={
    val sqlContext: SQLContext = new SQLContext(sc)
    val processing: RDDProcessing = new RDDProcessing()
    val rndSplit: RandomSplit = new RandomSplit("label", Array[Double](0.95, 0.05))
    val panRDD:RDD[PANDoc] = processing.panRDD(sc,printBuffer)
    val dtframe = sqlContext.createDataFrame(panRDD, classOf[PANDoc])

    val trainTestDf: Array[DataFrame] = rndSplit.randomSplit(dtframe, printBuffer)
    val trainFrame: DataFrame = trainTestDf(0).cache
    val testFrame: DataFrame = trainTestDf(1).cache


  }

}
