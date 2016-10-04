package data.document

import language.morphology.{MorphLight, MorphResult, AnalyzerImp}
import language.tokenization.{MyTokenizer, TokenizerImp}

/**
  * Created by wolf on 13.04.2016.
  */
case class Sentence(var paragraphid: Int,var author:String, var index:Int, var text:String) extends Serializable {


  def this(paragraphid:Int, index:Int, text:String) = this(paragraphid,null,index,text)


  def tokens(myTokenizer: TokenizerImp) : Seq[String] = {
    myTokenizer.tokenize(text)
  }

  def morphs(myTokenizer: TokenizerImp, stemmer:AnalyzerImp): Seq[MorphLight] = {
    val tokenArr = tokens(myTokenizer).toArray
    stemmer.analyzeAsLightSeq(tokenArr)
  }



  def canEqual(other: Any): Boolean = other.isInstanceOf[Sentence]

  override def equals(other: Any): Boolean = other match {
    case that: Sentence =>
      (that canEqual this) &&
        paragraphid == that.paragraphid &&
        index == that.index
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(paragraphid, index)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }



  override def toString = s"Sentence($text)"
}
