package language.spelling

import java.util.Locale

import language.model.LanguageModel
import language.tokenization.{MyTokenizer, TokenizerImp}
import options.Resources

/**
  * Created by wolf on 26.03.2016.
  */
class SpellCorrection(val languageModel: LanguageModel, val tokenizer: TokenizerImp, val maxPenalty: Double, val ngram: Int) extends Serializable {
  val spellChecker = new SingleWordSpellChecker(maxPenalty)

  val tr = new Locale("tr")


  def this(tokenizerImp: TokenizerImp, lmFilename:String, maxPenalty: Double,ngram: Int) = this(new LanguageModel(lmFilename), tokenizerImp, maxPenalty, ngram)

  def fit(sentences: Seq[String]): SpellCorrectionModel = {

    val wordsseq = sentences.map(sentence => tokenizer.tokenize(sentence.toLowerCase(tr)))

    wordsseq.foreach(tokens => {
      fitSentence(tokens)
    })

    new SpellCorrectionModel(tokenizer, languageModel, spellChecker)
  }

  def fitSentence(tokens: Seq[String]): Unit = {

    for (n <- 1 to ngram) {
      val seq = tokens.sliding(n, 1).map(words => {
        words.mkString(" ")
      })
      seq.foreach(word => {
        if (word.toCharArray.forall(char => {Character.isAlphabetic(char) || Character.isWhitespace(char)})) {
          spellChecker.addWord(word)
        }
      })
    }
  }


}

class Span(var start: Int, var length: Int, var values: Seq[Token]) extends Serializable {

  def addValue(value: Token): this.type = {
    values = values :+ value
    this
  }

  def addValue(value: Seq[Token]): this.type = {
    values = values ++ value
    this
  }

  def first(): Token = {
    values.head
  }

  def last(): Token = {
    values.last
  }

  def setLength(value: Int): this.type = {
    if (value > length) length = value
    this
  }

  def setStart(index: Int): this.type = {
    start = index
    this
  }

  def contains(span: Span): Boolean = {
    span.start >= start && span.start + span.length <= start + length
  }

  def isNotEmpty(): Boolean = {
    !values.isEmpty
  }

  def toNodes(): Seq[Node] = {
    values.map(value => new Node(value.value))
  }

  /**
    * Refurbish this code, this code is wrong
    * @return
    */
  def merge(): this.type = {
    val sorted = values.sortBy(token => {
      token.start - token.length
    })

    var current = sorted(0)

    var correct = if (sorted.length == 1) Seq[Token](current) else Seq[Token]()
    var i = 1
    var len = sorted.length
    while (i < len) {
      val next = sorted(i)
      if (current.nextOf(next)) {
        current.value = current.value + " " + next.value
        current.length = current.length + next.length
        i = i + 1
      }
      else {
        correct = correct :+ current
        current = next
        i = i + 1
      }

    }




    values = correct
    this
  }

  override def toString = s"Span($start, $length, $values)"
}

class Token(var start: Int, var length: Int, var value: String) extends Serializable {

  override def toString = s"Token($value)"

  def nextOf(token: Token): Boolean = {
    token.start == start + length
  }

  def contains(token: Token): Boolean = {
    token.start >= start &&
      token.start + token.length <= start + length &&
      value.contains(token.value)
  }
}

class SpellCorrectionModel(val tokenizerImp: TokenizerImp, val lm: LanguageModel, val spellChecker: SingleWordSpellChecker) extends Serializable {
  /*
  1. Generate all possible sequences by edit distance filtered by a word language model
  2. Multiword expressions can be mapped to multi word expressions n to n mapping
   */

  val locale = new Locale("tr")
  val ngram = 3

  def correct(sentence: String): String = {
    val tokens = tokenizerImp.tokenize(sentence.toLowerCase(locale))
    correct(tokens)
  }

  protected def correct(tokens: Seq[String]): String = {
    var result = Seq[String]()
    var productions = Seq[Span]()
    for (i <- 1 to ngram) {
      val slided = tokens.sliding(i, 1).toSeq
      productions = productions ++ slided.zipWithIndex.map(pair => {
        val tokens = pair._1
        val index = pair._2
        val suggestions = spellChecker.suggest(tokens.mkString(" "))
        val span = new Span(index, 1, Seq())
        suggestions.foreach(word => {
          val length = word.split("\\s").length
          span.setLength(length)
          span.addValue(new Token(index, length, word))
        })

        span
      }).filter(span => {
        span.isNotEmpty()
      }).toList

      val debug = 0
    }

    val sorted = sort(productions)
    val corrected = generate(sorted)
    val sentences = produce(corrected)
    lm.scoreTokensMax(sentences)
  }

  protected def sort(productions: Seq[Span]): Seq[Span] = {
    val spans = productions.sortBy(span => span.start - span.length)
    var result = Seq[Span]()

    if (spans.isEmpty) return result

    var i = 0
    var j = 1
    var current = spans(i)
    result = result :+ current
    while (i < spans.length && j < spans.length) {

      val next = spans(j)

      if (current.contains(next)) {
        current.addValue(next.values)
        j = j + 1
      }
      else {
        i = j
        j = j + 1
        result = result :+ next
        current = next
      }
    }

    return result


  }

  protected def produce(productions: Seq[Span]): Seq[Seq[String]] = {
    val startNode = new Node("START")
    val current = startNode
    var currentNodes = Seq[Node](current)
    productions.foreach(span => {
      val nodes = span.toNodes()
      currentNodes.foreach(node => {
        node.addNext(nodes)
      })
      currentNodes = nodes;
    })
    val paths = startNode.depthEnd(Seq())
    paths.map(nodes => {
      nodes.drop(1).foldRight[Seq[String]](Seq())((node, seq) => {
        node.value.split("\\s") ++ seq
      })
    })
  }

  protected def generate(productions: Seq[Span]): Seq[Span] = {
    productions.foreach(span => {
      span.merge()
    })

    productions
  }
}

class Node(var value: String) {
  var nexts = Seq[Node]()
  var mark = false

  def addNext(next: Node): Node = {
    if (notExists(next)) {
      nexts = nexts :+ next
    }
    next
  }

  def addNext(next: Seq[Node]): this.type = {
    next.foreach(node => addNext(node))
    this
  }

  def isEmpty(): Boolean = {
    nexts.isEmpty
  }


  def notExists(next: Node): Boolean = {
    !nexts.contains(next)
  }


  def depthEnd(path: Seq[Node]): Seq[Seq[Node]] = {
    var paths = Seq[Seq[Node]]()

    if (isEmpty() && !mark) {
      paths = paths :+ (path :+ this)
      //mark = true
    }
    else {
      //val subnodes = nexts.filter(subnode => !subnode.mark)

      val npath = path :+ this
      nexts.foreach(subnode => {
        paths = paths ++ subnode.depthEnd(npath)
      })
      //mark = true
    }

    paths

  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Node]

  override def equals(other: Any): Boolean = other match {
    case that: Node =>
      (that canEqual this) &&
        value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


  override def toString = s"Node($value)"
}


object SpellCorrection {
  val penalty = 2.0
  val ngram = 3
  val mytokenizer = new MyTokenizer
  val spellCorrection = new SpellCorrection(mytokenizer,Resources.LMResource.modelLM5TRFilename,  penalty, ngram)

  def main(args: Array[String]) {
    val correction = spellCorrection.fit(Seq("geldiğini görenler olmuş", "geldiği şüpheli", "görenler olsun"))
      .correct("geldiğini görenler olumuş geldiği şüpheli")

    println(correction)

    /*val node = new Node("START")
    val next1A = new Node("A")
    val next1B = new Node("B")
    val next2X1 = new Node("X")
    val next2X2 = new Node("Y")
    val next3K1 = new Node("K")
    val next3T1 = new Node("T")
    val next3K2 = new Node("K")
    val next3T2 = new Node("T")

    node.addNext(next1A)
    node.addNext(next1B)
    next1A.addNext(next2X1)
    next1B.addNext(next2X2)
    next2X1.addNext(next3K1)
    next2X1.addNext(next3T1)
    next2X2.addNext(next3K2)
    next2X2.addNext(next3T2)

    val seq = node.depthEnd(Seq());*/
    val d = 0

  }

}
