package org.apache.spark.ml.feature

import java.io.{FileWriter, BufferedWriter, File}

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions.col

/**
 * Created by wolf on 02.11.2015.
 */
class WekaARFFSink {
  def sink(dataFrame: DataFrame): Unit = {
    var text = "@RELATION datasink\n"
    var classText = ""
    var featureSize = 0
    val labelSize = dataFrame.select("label").distinct().count().toInt
    println("LABEL SIZE: "+labelSize)

    var lines = ""
    var count = 0
    val rows:Array[Row] = dataFrame.select("features", "label").collect()
    rows.foreach(row => {
      val array = row(0).asInstanceOf[SparseVector];
      val label = row(1).asInstanceOf[Double]
      featureSize = array.toArray.size
      var line: String = ""

      array.foreachActive((index, value) => {
        line += index+" "+value+", "
      })


      line = "{"+line+featureSize+" Author"+label.toInt+"}"
      lines += line+"\n";
      count+=1
      println(count+"/"+rows.length)
    })

    for (i <- 0 until featureSize) {
      text += "@ATTRIBUTE att" + i + " NUMERIC\n"
    }

    for (i <- 0 until labelSize) {
      classText += ",Author" + i.toString
    }

    text += "@ATTRIBUTE class{" + classText.substring(1) + "}\n"
    text += "@DATA\n"
    text+=lines;

    val file = new File("weka-sink.arff")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }
}
