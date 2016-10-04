package data.reports

import java.io.{File, FileFilter, PrintWriter}

import data.dataset.{XMLParser, XYZParser}
import options.Resources.{DatasetResources, Global}
import options._

import scala.util.control.Breaks
import scala.xml.Elem

/**
  * Created by wolf on 23.07.2016.
  */
class ReportQuery(val description: String, var printSorted:Boolean = true) {

  var goals = Set[String]()
  var differences = Set[String]()
  var groups = Set[String]()
  var equalDifference = false

  def setEqualDifference(value:Boolean):this.type ={
    equalDifference = value
    this
  }

  def filename():String={
    Global.reports + this.hashCode + ".xml"
  }


  def toXML(): String = {
    var xml = "<QUERY>\n"

    xml += ReportQueryXML.toXML("GOALS", "GOAL", goals)
    xml += ReportQueryXML.toXML("DIFFERENCES", "DIFFERENCE", differences)
    xml += ReportQueryXML.toXML("GROUPS", "GROUP", groups)

    xml + "</QUERY>\n"
  }

  def fromXML():this.type = {
    val xmlMain = scala.xml.XML.loadFile(filename);
    goals = ReportQueryXML.fromXML(xmlMain, "GOALS")
    differences = ReportQueryXML.fromXML(xmlMain, "DIFFERENCES")
    groups = ReportQueryXML.fromXML(xmlMain, "GROUPS")
    this
  }

  def addGoal(value: String*): this.type = {
    goals = goals ++ value
    this
  }

  def addGoal(value: Set[String]): this.type = {
    goals = goals ++ value
    this
  }

  def addDifference(value: String*): this.type = {
    differences = differences ++ value
    this
  }

  def addDifference(value:Set[String]):this.type ={
    differences = differences ++ value
    this
  }

  def addGroup(value: String*): this.type = {
    groups = groups ++ value
    this
  }

  def addGroup(value: Set[String]): this.type = {
    groups = groups ++ value
    this
  }



  def writeCompareAsXML(): Unit = {
    val xml = compareToXML()
    new PrintWriter(filename()) {
      write(xml)
      close()
    }
  }


  def compareToXML(): String = {
    //read all report
    //create groups consider reports whether those group or not
    val reports = readAll()
    groups = groups -- differences
    val grouping = Array(Array[Report](reports.head))

    reports.tail.foreach(report => {
      addGroup(grouping, report)
    })

    val comparable = grouping.filter(p => p.length >= 1)
    var difference = diffGroup(comparable).toSeq
    if(printSorted) {
      difference = difference.sortBy { case ((rp1, rp2), map) => {
        val gp1 = rp1.goals(goals).head._2.toDouble
        val gp2 = rp2.goals(goals).head._2.toDouble
        val diff = if(map.isEmpty) -1 else +1
        diff * Math.max(gp1, gp2)
      }}.reverse

    }

    "<ROOT>\n" + toXML() + ReportQueryXML.toXML(goals, difference) + "</ROOT>"
  }


  protected def diffGroup(comparable: Array[Array[Report]]): Map[(Report, Report), Map[String, Array[String]]] = {
    var diffmap = Map[(Report, Report), Map[String, Array[String]]]()

    comparable.foreach(group => {
      for (i <- 0 until group.length - 1) {
        for (j <- i + 1 until group.length) {
          val report = group(i)
          val otherReport = group(j)
          val mp = report.difference(otherReport, differences)
          if (equalDifference && mp.size == differences.size) {
            diffmap = diffmap + ((report, otherReport) -> mp)
          }
          else if(!equalDifference){
            diffmap = diffmap + ((report, otherReport) -> mp)
          }
        }
      }
    })

    diffmap
  }

  protected def addGroup(grouping: Array[Array[Report]], report: Report): Unit = {
    var added = false
    val loop = new Breaks()
    loop.breakable {
      for (i <- 0 until grouping.length) {
        if (grouping(i).length == 0) {
          grouping(i) = grouping(i) :+ report
          loop.break()
        }
        else {
          for (j <- 0 until grouping(i).length) {
            val otherReport = grouping(i)(j)
            added = report.doGroup(otherReport, groups)
            if (added) {
              grouping(i) = grouping(i) :+ report
              loop.break()
            }
          }
        }
      }
    }
  }

  protected def readAll(): Seq[Report] = {
    val files = (new File(Global.evaluations)).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        val name = pathname.getName
        val ext = name.substring(name.lastIndexOf(".") + 1)
        pathname.isFile && ext.equals("xml")
      }
    })
    files.map(file => new Report(file.getPath))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ReportQuery]

  override def equals(other: Any): Boolean = other match {
    case that: ReportQuery =>
      (that canEqual this) &&
        goals == that.goals &&
        differences == that.differences &&
        groups == that.groups
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(goals, differences, groups)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


  override def toString = s"ReportQuery($description)"
}




object ReportQuery {

  val I_RDDID = "RDD-ID"
  val I_PROCESSID = "PROCESSED-ID"
  val I_EVALID = "EVAL-ID"
  val I_CLASSID = "CLASSIFICATION-ID"
  val I_FEATURESID = "FEATURES-ID"


  val M_F_MEASURE = "F-MEASURE"
  val M_PRECISION = "PRECISION"
  val M_RECALL = "RECALL"
  val M_WEIGHTED_FMEASURE = "WEIGHTED-FMEASURE"
  val M_WEIGHTED_PRECISION = "WEIGHTED-PRECISION"
  val M_WEIGHTED_RECALL = "WEIGHTED-RECALL"


  def goals(): Set[String] = {
    Set(M_F_MEASURE, M_PRECISION, M_RECALL, M_WEIGHTED_FMEASURE, M_WEIGHTED_PRECISION, M_WEIGHTED_RECALL)
  }

  def classifications(): Set[String] = {
    ParamsClassification.map()
      .get(ParamsClassification.name)
      .get
  }

  def features():Set[String]={
    ParamsFeatures.map()
      .get(ParamsFeatures.name)
      .get
  }

  def queryWeights(): Unit ={
    val template = new ReportQuery("Measure Affect of Weight Method in Twitter")
    template.setEqualDifference(true)
    template.addGoal(M_F_MEASURE, M_PRECISION, M_RECALL)
    template.addDifference("tfidf","chiSquare")
    template.addGroup("dtTreeAuthor",I_RDDID,I_CLASSID)

    template.writeCompareAsXML()
  }

  def main(args: Array[String]): Unit = {
    queryWeights()
  }

}

object ReportQueryXML {

  def toXML(tagGroup: String, tag: String, values: Set[String]): String = {
    values.foldLeft[String]("<" + tagGroup + ">\n")((xml, b) => {
      xml + "<" + tag + ">\n" + b + "\n</" + tag + ">\n"
    }) +
      "</" + tagGroup + ">\n"
  }

  def fromXML(xml: Elem, tagGroup: String): Set[String] = {
    val seq = (xml \\ tagGroup)
    seq.map(node => {
      node.text
    }).toSet
  }

  def toXML(goals: Set[String], difference: Seq[((Report, Report), Map[String, Array[String]])]): String = {
    difference.foldLeft[String]("<COMPARISONS>\n") { case (xml, ((rp1, rp2), diffmap)) => {
      xml + "<COMPARE>\n" +
        toGoalXML(goals, rp1) +
        toGoalXML(goals, rp2) +
        toDifferenceXML(rp1.evalID, rp2.evalID, diffmap) +
        "</COMPARE>"


    }
    } + "\n</COMPARISONS>"
  }


  def toDifferenceXML(rp1Eval: String, rp2Eval: String, map: Map[String, Array[String]]): String = {
    map.foldLeft[String]("<DIFFERENCES>\n") {
      case (xml, (tag, values)) => {
        xml +
          "<" + tag + ">\n" +
          "<DIFFERENCE NAME=\"" + rp1Eval + "\" VALUE=\"" + values(0) + "\"/>\n" +
          "<DIFFERENCE NAME=\"" + rp2Eval + "\" VALUE=\"" + values(1) + "\"/>\n" +
          "</" + tag + ">\n"
      }
    } +
      "</DIFFERENCES>"
  }

  def toGoalXML(goals: Set[String], report: Report): String = {
    val filter = if (goals.isEmpty) ReportQuery.goals() else goals
    val values = report.goals(filter)
    "<MEASURES LABEL=\"" + report.evalID + "\">" +
      toTagXML(values) +
      "</MEASURES>"
  }

  def toTagXML(tagValues: Map[String, String]): String = {
    tagValues.foldLeft[String]("")((xml, tagVal) => {
      xml +
        "<" + tagVal._1 + ">\n" + tagVal._2 + "</" + tagVal._1 + ">\n"
    })
  }


}

