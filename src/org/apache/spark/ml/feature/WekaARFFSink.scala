package org.apache.spark.ml.feature

import java.io.{FileWriter, BufferedWriter, File}
import java.util
import java.util.concurrent.{FutureTask, Callable, Future, Executors}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions.col

/**
  * Created by wolf on 02.11.2015.
  */
class WekaARRFThread(val index: Int, val size: Int, val full: Int) extends Callable[Boolean] {
  var text = ""
  val sz = Math.min(index + size, full)


  override def call(): Boolean = {
    for (i <- index to sz) {
      text += "@ATTRIBUTE att" + i + " NUMERIC\n"
    }

    return true
  }

}


class WekaARFFSink {

  val threadNum = 24


  private def sinkAllFeatures(size: Int): String = {
    var text = "";
    val stepsize = size / threadNum;
    val tasks = new util.ArrayList[WekaARRFThread]()
    val executors = Executors.newFixedThreadPool(threadNum)
    var crr = 1
    for (i <- 0 until threadNum) {
      val thread = new WekaARRFThread(crr, stepsize, size)
      tasks.add(thread)
      crr += stepsize+1
    }


    executors.invokeAll(tasks)
    executors.shutdown();

    while (!executors.isShutdown) {}

    for (i <- 0 until tasks.size()) {
      text += tasks.get(i).text
    }

    text
  }

  def sinkTrain(filename: String, dataFrame: DataFrame): List[String] = {
    var text = "@RELATION datasink\n"
    var classText = ""
    val featureSize = {
      val array = dataFrame.select("features").first()(0).asInstanceOf[SparseVector]
      array.toArray.length
    }
    val labelDistinct = dataFrame.select("authorid").distinct()


    val lineRDD = dataFrame.select("features", "authorid").map(
      row => {
        val array = row(0).asInstanceOf[SparseVector];
        val label = row(1).asInstanceOf[String]


        var line: String = ""

        array.foreachActive((index, value) => {
          line += index + " " + value + ", "
        })

        line = "{" + line + featureSize + " " + label.toString + "}"
        line
      }
    )


    val lines = lineRDD.reduce((line1, line2) => line1 + "\n" + line2)


    text += sinkAllFeatures(featureSize)

    val collectedLabels = labelDistinct.collect()
    collectedLabels.foreach(row => classText += "," + row.get(0).asInstanceOf[String])


    text += "@ATTRIBUTE class{" + classText.substring(1) + "}\n"
    text += "@DATA\n"
    text += lines;

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
    return collectedLabels.map(row => row(0).asInstanceOf[String]).toList
  }

  def sinkTest(filename: String, trainLabels: List[String], dataFrame: DataFrame): Unit = {
    var text = "@RELATION datasink\n"
    var classText = ""

    val featureSize = {
      val array = dataFrame.select("features").first()(0).asInstanceOf[SparseVector]
      array.toArray.length
    }


    val lineRDD = dataFrame.select("features", "authorid").map(
      row => {
        val array = row(0).asInstanceOf[SparseVector];
        val label = row(1).asInstanceOf[Double]


        var line: String = ""

        array.foreachActive((index, value) => {
          line += index + " " + value + ", "
        })

        line = "{" + line + featureSize + " Author-" + label.toInt + "}"
        (label, line)
      }
    )



    val reducedRDD = lineRDD.filter(pair => {
      val label = "Author" + pair._1.toInt.toString
      trainLabels.contains(label)
    }).map(pair => pair._2)

    //reducedRDD.foreach(line => println(line))

    if (reducedRDD.isEmpty()) println("Empty")
    else println("Non-empty")

    val lines = reducedRDD.reduce((line1, line2) => line1 + "\n" + line2)

    for (i <- 0 until featureSize) {
      text += "@ATTRIBUTE att" + i + " NUMERIC\n"
    }

    trainLabels.foreach(label => classText += "," + label)


    text += "@ATTRIBUTE class{" + classText.substring(1) + "}\n"
    text += "@DATA\n"
    text += lines;

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

}

