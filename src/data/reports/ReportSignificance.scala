package data.reports

import java.io.{File, FileFilter}

import options.Resources.Global
import org.apache.commons.math3.stat.inference.TTest
import org.apache.spark.ml.MainExecutor
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Vectors}
import org.apache.spark.mllib.random.{PoissonGenerator, RandomDataGenerator, RandomRDDs}
import org.apache.spark.mllib.stat.Statistics

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by wolf on 30.07.2016.
  */
class ReportSignificance(val minTestSize: Int, val sampleSize: Int = 10, val sampleTimes: Int = 10,
                         val upperScore: Double = 0.05) {
  //on exact evaluation id
  //group reborts by evalID
  //filter by enough number of evals
  //for example 50
  //choose maximum result and
  val reports = readAll()


  /**
    * Two vectors must be non-negative and equal sized
    *
    * @param baseline
    * @param observed
    * @return significant is true, non-significant is false
    */
  def testSignificance(baseline: mllib.linalg.Vector, observed: mllib.linalg.Vector): Boolean = {

    val result = Statistics.chiSqTest(observed, baseline)
    result.pValue < upperScore
  }

  def testSignificance(difference: mllib.linalg.Vector): Boolean = {
    val result = Statistics.chiSqTest(difference)
    result.pValue < upperScore
  }

  def testSignificance(difference: Array[Double]): Boolean = {
    val t = new TTest
    val p = t.t(0.0, difference)

    p < upperScore
  }


  def testSignificance(baseline: Array[Double], observed: Array[Double]): Boolean = {
    val size = math.min(baseline.length, observed.length)
    if (size < minTestSize) false
    else {
      val baseTake = baseline.take(size)
      val observTake = observed.take(size)
      val difference = baseline.zip(observTake).map(pair => math.abs(pair._1 - pair._2))
      //val diffVector = Vectors.dense(difference)

      testSignificance(difference)
    }
  }

  def testSignifance(baseEvalID: String, observedEvalID: String, goal: String): Boolean = {
    val bases = values(baseEvalID, goal)
    val observes = values(observedEvalID, goal)
    var noTest = false
    if (bases.length < minTestSize) {
      noTest = true
      println("Experiment size is not satisfied for baseEvalID:" + baseEvalID)
    }

    if (observes.length < minTestSize) {
      noTest = true
      println("Experiment size is not satisfied for observedEvalID:" + observedEvalID)
    }

    if (!noTest) testSignificance(bases, observes)
    else false

  }

  def reportSignifcance(group: String, goal: String): Map[(String, String, String), Boolean] = {
    //group all possible experiments by evalID
    //compare same for example processid, or classificationid
    //by signifcant test

    val mapGroup = reports.groupBy(r => r.getValue(group))
    val mapCompare = mapGroup.mapValues(reports => {
      reports.groupBy(r => r.evalID)
    })

    val mapFiltered = mapCompare.map { case (group, map) => {
      val filter = map.filter { case (evalID, reports) => {
        reports.length > minTestSize
      }
      }
      (group, filter)
    }
    }.filter { case (group, map) => map.size > 1 }

    //940026534
    mapFiltered.flatMap { case (groupVal, reportMap) => {
      val keys = reportMap.keySet
      val pairs = keys.zip(keys.tail)
      pairs.map { case (e1, e2) => {
        val re1s = values(reportMap.getOrElse(e1, Seq[Report]()), goal)
        val re2s = values(reportMap.getOrElse(e2, Seq[Report]()), goal)
        ((groupVal, e1, e2), testSignificanceBySample(re1s, re2s))
      }
      }
    }
    }
  }

  def testSignificanceBySample(base: Array[Double], observed: Array[Double]): Boolean = {
    val baseMean = base.sum / base.length
    val observedMean = observed.sum / observed.length
    val baseArr = base.map(f => f * 100)
    val observArr = observed.map(f => f * 100)
    val test = new TTest


    val sample1 = sampleAsIs(baseArr)
    val sample2 = sampleAsIs(observArr)
    val result = test.tTest(sample1, sample2, upperScore)
    //val significance = testSignificance(Vectors.dense(meanSample))
    //val significance = testSignificance(meanSample)

    result
  }

  protected def sampleMean(values: Array[Double], sampleSize: Int = 20, sampleTimes: Int = 30): Array[Double] = {
    var means = Array[Double]()
    for (i <- 0 until sampleTimes) {
      val samples: Array[Double] = takeSample(values, sampleSize)
      val mean = samples.sum / samples.length
      means = means :+ mean
    }
    means

  }

  protected def sampleDiffMean(baseline: Array[Double], observed: Array[Double], sampleSize: Int = 20, sampleTimes: Int = 30): Array[Double] = {
    var means = Array[Double]()
    for (i <- 0 until sampleTimes) {
      val baseSamples: Array[Double] = takeSample(baseline, sampleSize)
      val observedSamples: Array[Double] = takeSample(observed, sampleSize)
      val difference = baseSamples.zip(observedSamples).map(pair => math.abs(pair._1 - pair._2))

      //val mean = difference.sum / difference.length
      means = means ++ difference
    }
    means

  }

  protected def sampleAsIs(values: Array[Double], sampleSize: Int = 20, sampleTimes: Int = 30): Array[Double] = {
    var samplings = Array[Double]()
    for (i <- 0 until sampleTimes) {
      val samples: Array[Double] = takeSample(values, sampleSize)
      samplings = samplings ++ samples
    }

    samplings

  }

  protected def takeSample(a: Array[Double], sampleSize: Int, seed: Long = 37): Array[Double] = {
    val rnd = new Random(seed)
    val sample = for (i <- 1 to sampleSize;
                      r = (rnd.nextInt(a.length))) yield a(r)
    sample.toArray
  }

  protected def values(evalID: String, goal: String): Array[Double] = {
    reports.filter(p => p.evalID.equals(evalID))
      .map(r => r.getValue(goal))
      .filter(p => !p.equals("NONE")).map(d => d.toDouble)
      .toArray
  }

  protected def values(reportSeq: Seq[Report], goal: String): Array[Double] = {
    reportSeq.map(report => report.getValue(goal))
      .filter(p => !p.equals("NONE"))
      .map(d => d.toDouble)
      .toArray
  }

  protected def readAll(): Seq[Report] = {
    println("Reading reports...")
    val files = (new File(Global.evaluations)).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        val name = pathname.getName
        val ext = name.substring(name.lastIndexOf(".") + 1)
        pathname.isFile && ext.equals("xml")
      }
    })
    files.map(file => new Report(file.getPath))
  }
}


object BasicSignificance extends ReportSignificance(5, 25, 100, 0.05) {

  def main(args: Array[String]): Unit = {

    reportSignifcance(ReportQuery.I_PROCESSID, ReportQuery.M_WEIGHTED_FMEASURE).foreach { case ((value, ev1, ev2), significant) => {
      val text = "Grouping: " + value + " EvalBase: " + ev1 + " EvalObserved: " + ev2 + " Significance: " + significant
      println(text)
    }
    }
  }
}
