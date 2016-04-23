package language.morphology

import scala.util.Random

/**
  * Created by wolf on 14.04.2016.
  */
class MorphLight(var token:String) {
  var results = Seq[String]()
  var label:String = null
  def addAnalysis(analysis:String) : this.type ={

    results = results :+ analysis
    this
  }

  def setLabel(labelStr:String): this.type ={
    label = labelStr
    this
  }

  def shuffle() : this.type ={
    results = Random.shuffle(results)
    this
  }

}
