package org.apache.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.pipes.{IDFWeighter, TFIDF}
import org.apache.spark.mllib.MLUtils
import org.apache.spark.sql.SQLContext

/**
  * Created by wolf on 05.07.2016.
  */
class WeightingExample {


  def initLocal(): SparkContext = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("log4j.rootCategory").setLevel(Level.FINEST)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf()
      .setAppName("Stemming Parsing")
      .setMaster("local[36]")
      .set("spark.default.parallelism", "36")
      //.set("log4j.rootCategory", "INFO")
      .set("spark.executor.memory", "16g")
      .set("spark.driver.maxResultSize", "24g")
      .set("spark.driver.memory", "72g")

    return new SparkContext(conf)
  }

  def pipeline():Pipeline={
    var stages = Array[PipelineStage]()

    stages = stages :+ new IDFWeighter()
      .setInputCol("features")
        .setOutputCol("weights")
        .setLabelCol("label")
        .setWeightingModel(new TFIDF()).setAttributes(Array("a"))

    new Pipeline().setStages(stages)
  }

  def executor(sc:SparkContext): Unit ={
    val pipe = pipeline()
    val sql = new SQLContext(sc)
    val df = sql.read.format("libsvm").load("libsvm.txt")
    val model = pipe.fit(df)
    val tdf = model.transform(df)
    tdf.show(4,false)
  }
}

object WeightingExample{
  def main(args: Array[String]) {
    val ex = new WeightingExample()
    ex.executor(ex.initLocal())
  }
}
