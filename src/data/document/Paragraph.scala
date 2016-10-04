package data.document

import language.boundary.SentenceML
import language.morphology.{AnalyzerImp, MorphLight, MorphResult}
import language.syntax.SyntaxML
import language.tokenization.TokenizerImp

/**
  * Created by wolf on 13.04.2016.
  */
case class Paragraph(var docid:String,var author:String, var index:Int, var text:String) extends Serializable{

  def this(docid:String, index:Int, text:String) = this(docid,null,index,text)


  def toSentences(sentenceML: SentenceML) :Seq[Sentence] ={
    val sentenceTexts = sentenceML.fit(text)
    sentenceTexts.zipWithIndex.map{case(sentence, seq)=>{
      new Sentence(hashCode(), author, seq, sentence)
    }}
  }

  def toMorphs(sentenceML:SentenceML, tokenizer:TokenizerImp, stemmer:AnalyzerImp) : Seq[MorphLight] = {
    toSentences(sentenceML).flatMap(sentence=>sentence.morphs(tokenizer, stemmer))
  }

  def toTokens(sentenceML: SentenceML, tokenizer: TokenizerImp) : Seq[String]={
    toSentences(sentenceML).flatMap(sentence=>sentence.tokens(tokenizer))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Paragraph]

  override def equals(other: Any): Boolean = other match {
    case that: Paragraph =>
      (that canEqual this) &&
        docid == that.docid &&
        index == that.index
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(docid, index)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
