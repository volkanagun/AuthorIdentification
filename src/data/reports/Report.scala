package data.reports

import java.io.File

import data.dataset.XMLParser

/**
  * Created by wolf on 24.07.2016.
  */
class Report(val filename: String) {
  //read report instance in peaces

  val map = XMLParser.parseParameters(filename) ++
    XMLParser.parseEvaluations(filename)

  val name = (new File(filename)).getName
  val evalID = map.getOrElse("EVAL-ID","NONE")

  def getValue(tag: String): String = {
    map.getOrElse(tag, "NONE")
  }

  protected def keyFilter(groups: Set[String]): Map[String, String] = {
    if (groups.isEmpty) map
    else {
      map.filter { case (name, value) => {
        groups.contains(name)
      }
      }
    }
  }

  protected def keyInverseFilter(groups: Set[String]): Map[String, String] = {
    if (groups.isEmpty) map
    else {
      map.filter { case (name, value) => {
        !groups.contains(name)
      }
      }
    }
  }


  def doGroup(other: Report, set: Set[String]): Boolean = {
    val fmap = keyFilter(set)
    val fothermap = other.keyFilter(set)

    //Does have the same keys
    //No need but may be
    if (fmap.size != fothermap.size) return false

    //Does values equal?
    fothermap.forall { case (k1, v1) => {
      val v2 = fothermap.getOrElse(k1, v1)
      v2.equals(v1)
    }
    }
  }

  def difference(other: Report, set: Set[String]): Map[String, Array[String]] = {
    val fmap = keyFilter(set)
    val fothermap = other.keyFilter(set)

    var diffMap = Map[String, Array[String]]()
    var doClear = !((fmap.size == set.size) && (fothermap.size == set.size))

    if (doClear) diffMap
    else {
      fmap.foreach { case (k1, v1) => {
        val v2 = fothermap.getOrElse(k1, v1)
        if (!v1.equals(v2)) {
          diffMap = diffMap + (k1 -> Array(v1, v2))
        }
        else if (!fothermap.contains(k1)) {
          diffMap = diffMap + (k1 -> Array(v1, "None"))
          doClear = true
        }
      }
      }

      fothermap.foreach { case (k2, v2) => {
        val v1 = fmap.getOrElse(k2, v2)
        if (!v2.equals(v1)) {
          diffMap = diffMap + (k2 -> Array(v1, v2))
        }
        else if (!fmap.contains(k2)) {
          diffMap = diffMap + (k2 -> Array("None", v2))
          doClear = true
        }
      }
      }

      diffMap
    }
  }

  def goals(set: Set[String]): Map[String, String] = {
    keyFilter(set)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Report]

  override def equals(other: Any): Boolean = other match {
    case that: Report =>
      (that canEqual this) &&
        evalID == that.evalID
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(evalID)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


  override def toString = s"Report($evalID)"
}
