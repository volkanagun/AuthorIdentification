package language.parser.examples

import java.util

import language.parser.engine.{Engine, Match, Regex, Rule}


/**
  * Created by wolf on 09.08.2016.
  */
class RuleMasking {
  val engine = new Engine()
  val ruleNumber = RuleNumbers.define()
  val ruleAbbreviation = RuleAbbreviation.define()
  val ruleMoney = RuleMoney.define()

  engine.addRule(ruleNumber)
  engine.addRule(ruleAbbreviation)
  engine.addRule(ruleMoney)

  def tokenizeDebug(text: String): Array[String] = {
    val matches: util.List[Match] = engine.parse(text)
    val matchList: util.List[String] = new util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (matching <- matches) {
      matchList.add(matching.matchString(text) + "_" + matching.getLabel + "_" + matching.getStart + "/" + matching.getEnd)
    }
    matchList.toArray(new Array[String](0))
  }


}

object RuleMasking {
  def main(args: Array[String]): Unit = {
    val masker = new RuleMasking
    val sentence = "U.S. 1000.000.000,00$ 500 Ms. Sanya PhD. Ph.D.";
    masker.tokenizeDebug(sentence).foreach(masking => {
      println(masking)
    })
  }
}

object RuleNumbers {

  val regexNumber = new Regex("number", "(\\d+)", "$1")
  val regexNumberTail = new Regex("numberTail", "([\\.\\,]\\d+)", "$1")
  val regexDot = new Regex("dot", "(\\.)", "$1")
  val regexComma = new Regex("comma", "(\\,)", "$1")

  def define(): java.util.List[Rule] = {
    val ruleList = new util.ArrayList[Rule]()
    val ruleStart = Rule.createRule(regexNumber)
    val ruleTail = Rule.createRule(regexNumberTail)
    val ruleContinue = Rule.createRule("number", ruleStart, ruleTail)


    ruleList.add(ruleStart)
    ruleList.add(ruleTail)
    ruleList.add(ruleContinue)

    ruleList
  }
}

object RuleAbbreviation {

  val regexAbbreviation = new Regex("abbreviation", "(([A-ZİĞÜŞÇÖ]\\.)+)", "$1")
  val regexMeasures = new Regex("measures", "(mm|cm|m|km|dm|nm|yd|ft|in|mil)", "$1")
  val regexReputation = new Regex("addressing", "(phd|PhD\\.|[Mm]r\\.|[Mm]s\\.)", "$1")


  def define(): util.List[Rule] = {
    val ruleList = new util.ArrayList[Rule]()
    val ruleMinimum = Rule.createRule(regexAbbreviation)
    val ruleMeasures = Rule.createRule(regexMeasures)
    val ruleAddressing = Rule.createRule(regexReputation)

    ruleList.add(ruleMinimum)
    ruleList.add(ruleMeasures)
    ruleList.add(ruleAddressing)
    ruleList
  }
}

object RuleMoney {
  val regexNumber = RuleNumbers.regexNumber
  val regexSign = new Regex("sign", "(\\$|\\£|\\€)", "$1")


  def define(): util.List[Rule] = {
    val ruleList = new util.ArrayList[Rule]()
    val ruleLeft = Rule.createRule("money", Array(regexSign, regexNumber))
    val ruleRight = Rule.createRule("money", Array(regexNumber, regexSign))

    ruleList.addAll(ruleLeft)
    ruleList.addAll(ruleRight)
    ruleList
  }
}
