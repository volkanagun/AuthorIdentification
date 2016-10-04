package org.apache.spark.ml.classification

import options.ParamsEvaluation
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasMaxIter, HasThreshold, HasTol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib

import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DoubleType

import scala.util.Random

/**
  * Created by wolf on 03.09.2016.
  */
class TOptions() {
  var lambda = 1.0
  var lambda_u = 1.0
  var S = 10000
  var R = 0.5
  var Cp = 1.0
  var Cn = 1.0
  var epsilon = TOptions.EPSILON
  var cgitermax = TOptions.CGITERMAX
  var mfnitermax = TOptions.MFNITERMAX
}

object TOptions {
  val CGITERMAX = 10000
  val SMALLCGITERMAX = 10
  val EPSILON = 1E-6
  val BIGEPSILON = 0.01
  val RELATIVESTOPEPS = 1E-9
  val MFNITERMAX = 50
  val TSVMANNEALINGRATE = 1.5
  val TSVMLAMBDASMALL = 1E-5

}

class Delta() {
  /* used in line search */
  var delta = 0.0
  var index = 0
  var s = 0
}

class Data(var m: Int, var l: Int, var u: Int, var n: Int, var nz: Int) {
  def this() = this(0, 0, 0, 0, 0)

  var dataVal = Array[Double]()
  var rowptr = Array[Int]()
  var colind = Array[Int]()

  var Y = Array[Double]()
  var C = Array[Double]()

}

class SVMTransductiveModel(override val uid:String, weights:Array[Double])extends ClassificationModel[Vector, SVMTransductiveModel]  with Serializable with ClassifierParams
{
  def this(weights:Array[Double])= this(Identifiable.randomUID("transductive-model"), weights)

  override def numClasses: Int = 2

  override protected def predictRaw(features: Vector): Vector = {
    val m = TransductiveSVM.predict(weights, features)
    Vectors.dense(-m, m)
  }


  protected override def predict(features: Vector): Double = {
    val m = TransductiveSVM.predict(weights, features)
    if(m > 0.0) 1.0
    else 0.0
  }

  override def copy(extra: ParamMap): SVMTransductiveModel = ???
}


class SVMTransductiveClassifier(override val uid:String) extends Classifier[Vector, SVMTransductiveClassifier, SVMTransductiveModel] {
  def this() = this(Identifiable.randomUID("transductive-svm"))

  override def copy(extra: ParamMap): SVMTransductiveClassifier = defaultCopy(extra)

  def trainRatio(df:DataFrame):DataFrame={
    import org.apache.spark.sql.functions._
    val trainTest = df.randomSplit(ParamsEvaluation.trainTestSplit)
    val trainDF = trainTest(0)
    val testDF = trainTest(1)
    val modified = testDF.withColumn($(labelCol),lit(0.5))
    trainDF.unionAll(modified)
  }

  override protected def train(dataset: DataFrame): SVMTransductiveModel = {
    val df = trainRatio(dataset)
    val lpData = extractLabeledPoints(df)
      .map(
        lp=>{
          val label = if(lp.label == 0.5) 0.0 else if(lp.label==1.0) 1.0 else -1.0
          LabeledPoint(label, lp.features)
      })
    val weights = TransductiveSVM.train(lpData)

    new SVMTransductiveModel(weights)

  }
}

object TransductiveSVM {

  def predict(weights:Array[Double], features:mllib.linalg.Vector):Double={
    var sum = 0.0
    features.foreachActive{case(index, value)=>{
      if(value>0.0) sum += value * weights(index)
    }}

    sum += weights(weights.length-1)
    sum
  }

  def train(input:RDD[LabeledPoint], options:TOptions = new TOptions()):Array[Double]={
    val data = convertData(input)
    val weights = initialize(data.n,0.0)
    val outputs = initialize(data.m,0.0)
    tsvmMFN(data,options, weights, outputs)
    weights
  }

  protected def compressData(input:RDD[LabeledPoint]):Data={
    println("Compressing data values with bias")
    val lpFeatures = input.map(lp=>lp.features).collect()
    var array = Array[Double]()
    var ia = Array[Int](0)
    var ja = Array[Int]()
    var lst = 1
    var lstNonZero = 0
    var n = 0
    /*data.zipWithIndex.foreach{case(features, rw)=>{
      var numNonZero = 0
      n = features.size
      features.foreachActive{case (i:Int,f:Double)=>{
        if(f!=0.0){
          array = array :+ f
          ia = ia :+ (ia(lst-1)+lstNonZero)
          ja = ja :+ i
          numNonZero += 1
        }
      }}
      lstNonZero = numNonZero

    }}
    */

    //append bias for each sample to the last row
    val cLen = lpFeatures.head.size
    val nz = cLen*lpFeatures.length + lpFeatures.length
    val Val = initialize(nz, 0.0)
    val C = initialize(nz, 0)
    val R = initialize(lpFeatures.length+1, 0)
    val m = lpFeatures.length
    var t = 0
    var j = 0

    for(i<-0 until m){
      R(t) = j
      t += 1
      val vector = lpFeatures(i)
      for(k<-0 until vector.size){
        Val(j) = vector.apply(k)
        C(j) = k

        j += 1
      }

      n = vector.size
      C(j) = 0
      Val(j) = 1.0
      j += 1
    }

    R(m) = nz
    n+=1

    for(i<-0 until m){
      C(R(i+1)-1) = n
    }
    n+=1

    val data = new Data()
    data.n = n
    data.m = m
    data.dataVal = Val
    data.rowptr = R
    data.colind = C
    data.nz = nz


    data

  }



  protected def convertData(input: RDD[LabeledPoint]): Data = {

    println("Converting dataset to TSVM format...")


    val labels = input.map(lp=>lp.label).collect()
    val posCount = labels.filter(lp => lp == 1.0).length
    val negCount = labels.filter(lp => lp == -1.0).length
    val numFeatures = input.first().features.size
    val data = compressData(input)


    data.l = (posCount + negCount)
    data.u = (labels.length - data.l)


    data.C = initialize(data.m, 1.0)
    data.Y = labels

    data

  }

  def initialize(k:Int) = (1 to k).toArray

  def initialize(k: Int, a: Double): Array[Double] = Array.fill[Double](k)(a)

  def initialize(k: Int, a: Int): Array[Int] = Array.fill[Int](k)(a)

  def labelledData(D: Data, data: Data): Unit = {
    val J = initialize(data.l, 0)
    D.C = initialize(data.l, 0.0)
    D.Y = initialize(data.l, 0.0)
    var nz = 0
    var k = 0
    val rowptrs = data.l
    for (i <- 0 until data.m) {
      if (data.Y(i) != 0.0) {
        J(k) = i;
        D.Y(k) = data.Y(i);
        D.C(k) = 1.0 / data.l;
        nz += (data.rowptr(i + 1) - data.rowptr(i));
        k += 1;
      }
    }

    D.dataVal = initialize(nz, 0.0)
    D.colind = initialize(nz, 0)
    D.rowptr = initialize(rowptrs + 1, 0)

    nz = 0
    for (i <- 0 until data.l) {
      D.rowptr(i) = nz
      for (j <- data.rowptr(J(i)) until data.rowptr(J(i) + 1)) {
        D.dataVal(nz) = data.dataVal(j)
        D.colind(nz) = data.colind(j)
        nz += 1
      }
    }

    D.rowptr(rowptrs) = nz


    D.nz = nz
    D.l = data.l
    D.m = data.l
    D.n = data.n
    D.u = 0
  }

  def cgls(data: Data, options: TOptions, subset: Array[Int], subsetLength: Int, weights: Array[Double], outputs: Array[Double]): Int = {


    val active = subsetLength
    val J = subset
    val dataVal = data.dataVal
    val row = data.rowptr
    val col = data.colind
    val Y = data.Y
    val C = data.C
    val n = data.n
    val lambda = options.lambda
    val cgitermax = options.cgitermax
    val epsilon = options.epsilon
    val beta = weights
    val o = outputs

    //initialize z
    val z = initialize(active, 0.0)
    val q = initialize(active, 0.0)
    var ii = 0
    for (i <- active-1 to 0 by -1) {
      ii = J(i)
      z(i) = C(ii) * (Y(ii) - o(ii))
    }

    val r = initialize(n, 0.0)

    for (j <- 0 until active) {
      ii = J(j)
      for (i <- row(ii) until row(ii + 1)) {
        r(col(i)) += dataVal(i) * z(j)
      }
    }

    val p = initialize(n, 0.0)
    var omega1 = 0.0
    for (i <- n-1 to 0 by -1) {
      r(i) -= lambda * beta(i)
      p(i) = r(i)
      omega1 += r(i) * r(i)
    }

    var omegap = omega1
    var omegaq = 0.0
    var invomega2 = 1 / omega1
    var scale = 0.0
    var omegaz = 0.0
    var gamma = 0.0
    var cgiter = 0
    var optimality = 0
    var epsilon2 = epsilon * epsilon

    //iterate

    breakable {
      while (cgiter < cgitermax) {
        cgiter += 1
        omegaq = 0.0
        var t = 0.0
        var i = 0
        var j = 0

        for (i <- 0 until active) {
          ii = J(i)
          t = 0.0
          for (j <- row(ii) until row(ii + 1)) {
            t += dataVal(j) * p(col(j))
          }
          q(i) = t
          omegaq += C(ii) * t * t
        }

        gamma = omega1 / (lambda * omegap + omegaq)
        invomega2 = 1 / omega1

        for (i <- n-1 to 0 by -1) {
          r(i) = 0.0
          beta(i) += gamma * p(i)
        }

        omegaz = 0.0
        for (i <- active-1 to 0 by -1) {
          ii = J(i)
          o(ii) += gamma * q(i)
          z(i) -= gamma * C(ii) * q(i)
          omegaz += z(i) * z(i)
        }

        for (j <- 0 until active) {
          ii = J(j)
          t = z(j)
          for (i <- row(ii) until row(ii + 1)) {
            r(col(i)) += dataVal(i) * t
          }
        }

        omega1 = 0.0
        for (i <- n-1 to 0 by -1) {
          r(i) -= lambda * beta(i)
          omega1 += r(i) * r(i)
        }

        if (omega1 < epsilon2 * omegaz) {
          optimality = 1
          break()
        }

        omegap = 0.0
        scale = omega1 * invomega2
        for (i <- n-1 to 0 by -1) {
          p(i) = r(i) + p(i) * scale
          omegap += p(i) * p(i)
        }
      }
    }

    optimality
  }

  def lineSearch(w: Array[Double], weightsBar: Array[Double], lambda: Double, o: Array[Double],
                 outputsBar: Array[Double], Y: Array[Double], C: Array[Double], d: Int, l: Int): Double = {


    var omegaL = 0.0
    var omegaR = 0.0
    var diff = 0.0

    for (i <- d-1 to 0 by -1) {
      diff = weightsBar(i) - w(i)
      omegaL += w(i) * diff
      omegaR = weightsBar(i) * diff
    }

    omegaL = lambda * omegaL
    omegaR = lambda * omegaR

    var L = 0.0
    var R = 0.0
    var ii = 0
    for (i <- 0 until l) {
      if (Y(i) * o(i) < 1) {
        diff = C(i) * (outputsBar(i) - o(i))
        L += (o(i) - Y(i)) * diff
        R += (outputsBar(i) - Y(i)) * diff
      }
    }

    L += omegaL
    R += omegaR
    var deltas = Array.fill[Delta](l)(new Delta())
    var p = 0

    for (i <- 0 until l) {
      diff = Y(i) * (outputsBar(i) - o(i))
      if (Y(i) * o(i) < 1) {
        if (diff > 0) {
          deltas(p).delta = (1 - Y(i) * o(i)) / diff
          deltas(p).index = i
          deltas(p).s = -1
          p += 1
        }
      }
      else {
        if (diff < 0) {
          deltas(p).delta = (1 - Y(i) * o(i)) / diff
          deltas(p).index = i
          deltas(p).s = -1
          p += 1
        }
      }
    }

    deltas = deltas.sortBy(delta => delta.delta)
    var deltaPrime = 0.0
    breakable {
      for (i <- 0 until p) {
        deltaPrime = L + deltas(i).delta * (R - L)
        if (deltaPrime >= 0) break()

        ii = deltas(i).index
        diff = (deltas(i).s) * C(ii) * (outputsBar(ii) - o(ii))
        L += diff * (o(ii) - Y(ii))
        R += diff * (outputsBar(ii) - Y(ii))

      }
    }

    -L / (R - L)
  }

  def l2svmMFN(data: Data, options: TOptions, weights: Array[Double], outputs: Array[Double], ini: Int): Int = {

    var gini = ini
    val dataVal = data.dataVal
    val row = data.rowptr
    val col = data.colind
    val Y = data.Y
    val C = data.C
    val n = data.n
    val m = data.m
    var lambda = options.lambda
    var epsilon = 0.0
    val w = weights
    val o = outputs
    var Fold = 0.0
    var F = 0.0
    var diff = 0.0
    val activeSubset = initialize(m, 0)
    if (gini == 0) {
      epsilon = TOptions.BIGEPSILON
      options.cgitermax = TOptions.SMALLCGITERMAX
      options.epsilon = TOptions.BIGEPSILON
    }
    else {
      epsilon = options.epsilon
    }

    for (i <- 0 until n) F += w(i) * w(i)
    F = 0.5 * lambda * F
    var active = 0
    var inactive = m - 1

    for (i <- 0 until m) {
      diff = 1 - Y(i) * o(i)
      if (diff > 0) {
        activeSubset(active) = i
        active += 1
        F += 0.5 * C(i) * diff * diff
      }
      else {
        activeSubset(inactive) = inactive
        inactive -= 1
      }
    }

    var subsetLength = active
    var iter = 0
    var opt = 0
    var opt2 = 0
    val weightsBar = initialize(n, 0.0)
    val outputsBar = initialize(m, 0.0)

    var delta = 0.0
    var t = 0.0
    var ii = 0

    while (iter < TOptions.MFNITERMAX) {
      iter += 1

      for (i <- m-1 to 0 by -1) {
        outputsBar(i) = o(i)
      }

      for (i <- n-1 to 0 by -1) {
        weightsBar(i) = w(i)
      }


      opt = cgls(data, options, activeSubset, subsetLength, weightsBar, outputsBar)

      for (i <- active until m) {
        ii = activeSubset(i)
        t = 0.0
        for (j <- row(ii) until row(ii + 1)) {
          t += dataVal(j) * weightsBar(col(j))
        }
        outputsBar(ii) = t
      }


      if (gini == 0) {
        options.cgitermax = TOptions.CGITERMAX
        gini = 1
      }
      opt2 = 1
      breakable {
        for (i <- 0 until m) {
          ii = activeSubset(i)
          if (i < active) opt2 = (if (opt2 == 1 && (Y(ii) * outputsBar(ii) <= 1 + epsilon)) 1 else 0)
          else opt2 = (if (opt2 == 1 && (Y(ii) * outputsBar(ii) >= 1 - epsilon)) 1 else 0)

          if (opt2 == 0) break()
        }
      }

      if (opt == 1 && opt2 == 1) {
        if (epsilon == TOptions.BIGEPSILON) {
          epsilon = TOptions.EPSILON
          options.epsilon = epsilon
        }
        else {
          for (i <- n-1 to 0 by -1)
            w(i) = weightsBar(i)
          for (i <- m-1 to 0 by -1)
            o(i) = outputsBar(i)

          return 1
        }
      }

      delta = lineSearch(w, weightsBar, lambda, o, outputsBar, Y, C, n, m)
      Fold = F
      F = 0.0

      for (i <- n-1 to 0 by -1) {
        w(i) += delta * (weightsBar(i) - w(i))
        F += w(i) * w(i)
      }

      F = 0.5 * lambda * F
      active = 0
      inactive = m - 1
      for (i <- 0 until m) {
        o(i) += delta * (outputsBar(i) - o(i))
        diff = 1 - Y(i) * o(i)
        if (diff > 0) {
          activeSubset(active) = i
          active += 1
          F += 0.5 * C(i) * diff * diff

        }
        else {
          activeSubset(inactive) = i
          inactive -= 1
        }
      }

      if (math.abs(F - Fold) < TOptions.RELATIVESTOPEPS * math.abs((Fold))) {
        return 2
      }

    }

    return 0
  }

  def switchLabels(Y: Array[Double], o: Array[Double], JU: Array[Int], u: Int, S: Int): Int = {
    var npos = 0
    var nneg = 0
    for (i <- 0 until u) {
      if (Y(JU(i)) > 0 && o(JU(i)) < 1.0) npos += 1
      if (Y(JU(i)) < 0 && -o(JU(i)) < 1.0) nneg += 1
    }

    var positive = Array.fill[Delta](npos)(new Delta())
    var negative = Array.fill[Delta](nneg)(new Delta())
    var p = 0
    var n = 0
    var ii = 0

    for (i <- 0 until u) {
      ii = JU(i)
      if (Y(ii) > 0.0 && o(ii) < 1.0) {
        positive(p).delta = o(ii)
        positive(p).index = ii
        positive(p).s = 0
        p += 1
      }
      if (Y(ii) < 0.0 && -o(ii) < 1.0) {
        negative(n).delta = -o(ii)
        negative(n).index = ii
        negative(n).s = 0
        n += 1
      }
    }

    positive = positive.sortBy(delta => delta.delta)
    negative = negative.sortBy(delta => delta.delta)
    var s = -1

    breakable(while (true) {
      s += 1
      if (s >= S || s >= npos || s >= nneg || positive(s).delta >= -negative(s).delta) break()
      Y(positive(s).index) = -1.0
      Y(negative(s).index) = 1.0
    })

    s
  }

  def transductiveCost(normWeights: Double, Y: Array[Double], outputs: Array[Double], m: Int, lambda: Double, lambdau: Double): Double = {

    var u = 0
    var l = 0
    var (f1, f2, o, y) = (0.0, 0.0, 0.0, 0.0)
    for (i <- 0 until m) {
      o = outputs(i)
      y = Y(i)
      if (y == 0.0) {
        f1 += (if (math.abs(o) > 1) 0.0 else (1 - math.abs(o)) * (1 - math.abs(o)))
        u += 1
      }
      else {
        f2 += (if (y * o > 1) 0.0 else (1 - y * o) * (1 - y * o))
        l += 1
      }
    }

    val f = 0.5 * (lambda * normWeights + lambdau * f1 / u + f2 / l)
    f
  }

  def normSquare(vector: Array[Double]): Double = {
    var x = 0.0
    var t = 0.0
    for (i <- 0 until vector.length) {
      t = vector(i)
      x += t * t
    }

    x
  }

  def tsvmMFN(data: Data, options: TOptions, weights: Array[Double], outputs: Array[Double]): Int = {
    val dataLabelled = new Data()
    val outputsLabelled = initialize(data.l, 0.0)
    labelledData(dataLabelled, data)
    l2svmMFN(dataLabelled, options, weights, outputsLabelled, 0)
    var p = 0
    var q = 0
    var t = 0.0
    var JU = initialize(data.u, 0)
    var ou = initialize(data.u, 0.0)
    var lambda0 = TOptions.TSVMLAMBDASMALL
    for (i <- 0 until data.m) {
      if (data.Y(i) == 0.0) {
        t = 0.0
        for (j <- data.rowptr(i) until data.rowptr(i + 1)) {
          t += data.dataVal(j) * weights(data.colind(j))
        }
        outputs(i) = t
        data.C(i) = lambda0 * 1.0 / data.u
        JU(q) = i
        ou(q) = t
        q += 1
      }
      else {
        outputs(i) = outputsLabelled(p)
        data.C(i) = 1.0 / data.l
        p += 1
      }
    }

    val threshold = nthElement(ou, ((1 - options.R) * data.u - 1).toInt)
    for (i <- 0 until data.u) {
      if (outputs(JU(i)) > threshold) {
        data.Y(JU(i)) = 1.0
      }
      else {
        data.Y(JU(i)) = -1.0
      }
    }

    for (i <- 0 until data.n) {
      weights(i) = 0.0
    }
    for (i <- 0 until data.m) {
      outputs(i) = 0.0
    }

    l2svmMFN(data, options, weights, outputs, 0)
    var numSwitches = 0
    var s = 0
    var lastRound = 0

    breakable(while (lambda0 <= options.lambda_u) {
      var iter2 = 0
      breakable(while (true) {
        s = switchLabels(data.Y, outputs, JU, data.u, options.S)
        if (s == 0) break()
        iter2 += 1
        println("Optimizing unknown labels. switched " + s + " labels.")
        numSwitches += s
        println("Optimizing weights")
        l2svmMFN(data, options, weights, outputs, 1)
      })

      if (lastRound == 1) break()
      lambda0 = TOptions.TSVMANNEALINGRATE * lambda0
      if (lambda0 >= options.lambda_u) {
        lambda0 = options.lambda_u
        lastRound = 1
      }

      for (i <- 0 until data.u) {
        data.C(JU(i)) = lambda0 * 1.0 / data.u
        println("Optimizing weights")
        l2svmMFN(data, options, weights, outputs, 1)
      }
    })

    for (i <- 0 until data.u) {
      data.Y(JU(i)) = 0.0
    }

    val f = transductiveCost(normSquare(weights), data.Y, outputs,
      outputs.length,
      options.lambda,
      options.lambda_u)

    numSwitches
  }

  def quickSelect[A <% Ordered[A]](seq: Array[A], n: Int, rand: Random = new Random): A = {
    val pivot = rand.nextInt(seq.length);
    val (left, right) = seq.partition(_ < seq(pivot))
    if (left.length == n) {
      seq(pivot)
    } else if (left.length < n) {
      quickSelect(right, n - left.length, rand)
    } else {
      quickSelect(left, n, rand)
    }
  }

  def nthElement[A <% Ordered[A]](seq: Array[A], n: Int):A={
    for (i<-0 to n) {
      var minIndex = i
      var minValue = seq(i)
      for(j<-i+1 until seq.length)
      if (seq(j) < minValue) {
        minIndex = j
        minValue = seq(j)
        val ivalue = seq(i)
        val mvalue = seq(minIndex)
        seq(i) = mvalue
        seq(minIndex) = ivalue

      }
    }

    seq(n)
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val arr = Array(5, 6, 4, 3, 2, 6, 7, 9, 3)
    println(TransductiveSVM.nthElement(arr, arr.length-2))
  }
}