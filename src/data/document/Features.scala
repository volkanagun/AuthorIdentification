package data.document

import java.util.Locale

import language.morphology.AnalyzerImp
import options._

import scala.util.matching.Regex

/**
  * Created by wolf on 14.04.2016.
  */
class Window[I](val windowSize: Int) extends Serializable {

  /**
    *
    * @param sequence : parent id, sequence id, features
    * @return parent id, sequence id, current sequence features, and window features
    */
  def window(sequence: Seq[(Int, Int, I, Seq[String])]): Seq[(Int, Int, I, Seq[String], Seq[String])] = {
    val windowFeatures = sequence.par.map { case (pid, sid, item, features) => {
      val min = Math.max(0, sid - windowSize)
      val max = Math.min(sequence.length - 1, sid + windowSize)
      val windowFeatures = (for (i <- min to max;
                                 diff = i - sid;
                                 win = if (diff > 0) "+" + diff.toString else diff.toString();
                                 mfeatures = sequence(i)._4)
        yield mfeatures.map(feature => win + feature)).flatten

      (pid, sid, item, features, windowFeatures)
    }
    }.seq

    windowFeatures
  }
}

class Context() extends Serializable {


  def ngramsString(tokens: Seq[String], min: Int, max: Int): Seq[String] = {

    var features = Seq[String]()
    for (i <- min to max) {
      features = features ++ tokens.iterator.sliding(i)
        .withPartial(false)
        .map(_.mkString(" "))
    }
    features

  }

  def ngramsChar(tokens: Seq[Char], min: Int, max: Int): Seq[String] = {

    var features = Seq[String]()
    for (i <- min to max) {
      features = features ++ tokens.iterator.sliding(i)
        .withPartial(false)
        .map(_.mkString(" "))
    }
    features

  }

  /**
    *
    * @param sequence token and label
    * @param sid      sentence id
    * @return
    */
  def tokenEmptyFeatures(sequence: Seq[(String, String)], sid: Int = 0): Seq[(Int, Int, String, Seq[String])] = {
    sequence.zipWithIndex.map { case ((token, label), indice) => {
      (sid, indice, token, Seq[String]())
    }
    }
  }

  /**
    *
    * @param sequence token, label
    * @return
    */
  def tokenLabelFeatures(sequence: Seq[(String, String)]): Seq[(Int, Int, String, Seq[String])] = {
    val pid = 0
    val analyzer = ParamsMorphology.analyzer
    sequence.zipWithIndex.map { case ((token, label), indice) => {
      val features = TokenShape.shapes(token) ++
        TokenShape.emoticons(token) ++
        TokenShape.suffixes(token, "SUFFIX", ParamsTokenChar.maxSuffixLength) ++
        TokenShape.prefixes(token, "PREFIX", ParamsTokenChar.maxPrefixLength) ++
        TokenStem.stem(analyzer, token) ++
        TokenStem.stemSuffix(analyzer, token, ParamsTokenChar.maxSuffixLength) ++
        TokenStem.stemPrefix(analyzer, token, ParamsTokenChar.maxPrefixLength) ++
        TokenShape.self(token)
      (pid, indice, token, features)
    }
    }
  }

  def sentenceTokenFeatures(): Seq[String] => Seq[String] = {
    val analyzer = ParamsMorphology.analyzer
    tokens: Seq[String] => {
      tokens.zipWithIndex.flatMap { case (token, indice) => {
        TokenShape.shapes(token) ++
          TokenShape.emoticons(token) ++
          TokenShape.suffixes(token, "SUFFIX", ParamsTokenChar.maxSuffixLength) ++
          TokenShape.prefixes(token, "PREFIX", ParamsTokenChar.maxPrefixLength) ++
          TokenStem.stem(analyzer, token) ++
          TokenStem.stemSuffix(analyzer, token, ParamsTokenChar.maxSuffixLength) ++
          TokenStem.stemPrefix(analyzer, token, ParamsTokenChar.maxPrefixLength) ++
          TokenShape.self(token)

      }
      }
    }
  }



  def sentenceTokenGramFeatures(): Seq[String] => Seq[String] = {
    val max = ParamsTokenGram.maxTokenGramLength
    val min = ParamsTokenGram.minTokenGramLength

    tokens: Seq[String] => {
      ngramsString(tokens,min,max)
    }
  }

  def sentencePOSGramFeatures(): Seq[String] => Seq[String] = {
    val max = ParamsTokenGram.maxPOSGramLength
    val min = ParamsTokenGram.minPOSGramLength

    poses:Seq[String]=>{
      ngramsString(poses,min,max)
    }
  }

  def textRepeatedPuncFeatures() : String=>Seq[String]= {
    val puncpattern = new Regex("([\\p{Punct}\\@\\'\\,\\&])(\\1)+")
    sentence:String=>{puncpattern.findAllIn(sentence).toSeq}
  }

  def textCharGramFeatures(): String => Seq[String] = {
    val max = ParamsSentenceChar.maxCharGramLength
    val min = ParamsSentenceChar.minCharGramLength

    sentence: String => {
      val chars = sentence.toCharArray.toSeq
      ngramsChar(chars, min, max)

    }
  }

  def turkishPhrasels(): String=>Seq[String] ={
    val fun:String => Seq[String] = {
      var features = Seq[String]()
      sentence:String=>{
        features = features ++ Phrasels.turkishStart(sentence)
        features = features ++ Phrasels.turkishEnd(sentence)
        features = features ++ Phrasels.turkishContains(sentence)
        features
      }
    }

    fun
  }

}


//////////////////////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Stem/Suffix Features">

object TokenStem {

  def stem(analyzer: AnalyzerImp, token: String): Seq[String] = {
    val stem = analyzer.stem(token)
    var features = Seq("STEM=" + stem)

    if (stem.length < token.length) {
      features = features :+ "SUFFIX=" + token.substring(stem.length)
    }

    features
  }

  def stem(analyzer: AnalyzerImp, features: Seq[String], token: String): Seq[String] = {
    features :+ "STEM=" + analyzer.stem(token)
  }

  def stemSuffix(analyzer: AnalyzerImp, token: String, suffixLength: Int): Seq[String] = {
    val features = Seq[String]()
    stemSuffix(analyzer, features, token, suffixLength)
  }

  def stemSuffix(analyzer: AnalyzerImp, features: Seq[String], token: String, suffixLength: Int): Seq[String] = {
    val stm = analyzer.stem(token)
    TokenShape.suffixes(features, stm, "SSUFIX", suffixLength)
  }

  def stemPrefix(analyzer: AnalyzerImp, token: String, prefixLength: Int): Seq[String] = {
    val features = Seq[String]()
    stemPrefix(analyzer, features, token, prefixLength)
  }

  def stemPrefix(analyzer: AnalyzerImp, features: Seq[String], token: String, suffixLength: Int): Seq[String] = {
    val stm = analyzer.stem(token)
    TokenShape.prefixes(features, stm, "SPREFIX", suffixLength)
  }

}

//</editor-fold>
//////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Token Shape Features">
//////////////////////////////////////////////////////////////////////////

object TokenShape {

  lazy val locale = new Locale("tr")
  lazy val emoticons = Dictionary.loadEmoticons()

  def self(token: String): Seq[String] = {
    Seq(token, token.toLowerCase(locale))
  }

  def self(features: Seq[String], token: String): Seq[String] = {
    features :+ token :+ token.toLowerCase(locale)
  }

  def shapes(token: String): Seq[String] = {
    val features = Seq[String]()
    shapes(features, token)
  }

  def shapes(features: Seq[String], token: String): Seq[String] = {
    if (token.isEmpty) features
    else {

      val first = token.head
      val last = token.last

      var nfeatures = features

      if (Character.isAlphabetic(first)) {
        nfeatures = nfeatures :+ "START-ALPHA"
      }

      if (Character.isAlphabetic(last)) {
        nfeatures = nfeatures :+ "END-ALPHA"
      }

      if (!Character.isLetterOrDigit(first)) {
        nfeatures = nfeatures :+ "START-NON"
      }

      if (!Character.isLetterOrDigit(last)) {
        nfeatures = nfeatures :+ "END-NON"
      }

      if (Character.isDigit(first)) {
        nfeatures = nfeatures :+ "START-DIGIT"
      }

      if (Character.isDigit(last)) {
        nfeatures = nfeatures :+ "END-DIGIT"
      }

      if (Character.isLowerCase(first)) {
        nfeatures = nfeatures :+ "START-LOWERCASE"
      }

      if (Character.isUpperCase(first)) {
        nfeatures = nfeatures :+ "START-UPPERCASE"
      }

      if (token.contains(":")) {
        nfeatures = nfeatures :+ "CONTAINS-SEMICOLON"
      }

      if (token.contains("-") || token.contains("_")) {
        nfeatures = nfeatures :+ "CONTAINS-DASH"
      }

      if (token.contains(".") || token.contains("?") || token.contains("!")) {
        nfeatures = nfeatures :+ "CONTAINS-STOPPING"
      }

      if (token.contains("/") || token.contains("\\")) {
        nfeatures = nfeatures :+ "CONTAINS-DIVIDE"
      }

      if (token.forall(char => Character.isDigit(char))) {
        nfeatures = nfeatures :+ "ALL-DIGIT"
      }

      if (token.forall(char => Character.isUpperCase(char))) {
        nfeatures = nfeatures :+ "ALL-UPPERCASE"
      }

      if (token.forall(char => Character.isLowerCase(char))) {
        nfeatures = nfeatures :+ "ALL-LOWERCASE"
      }

      nfeatures
    }
  }

  def emoticons(token: String): Seq[String] = {
    val features = Seq[String]()
    emoticons(features, token)
  }

  def emoticons(features: Seq[String], token: String): Seq[String] = {
    if (token.isEmpty) features
    else {
      var nfeatures = features

      if (emoticons.contains(token.trim)) {
        nfeatures = nfeatures :+ "EMOTICON"
      }
      nfeatures
    }
  }

  def suffixes(token: String, featLabel: String, suffixLength: Int): Seq[String] = {
    val features = Seq[String]()
    suffixes(features, token, featLabel, suffixLength)
  }

  def suffixes(features: Seq[String], token: String, featLabel: String, suffixLength: Int): Seq[String] = {
    val length = token.length
    val start = Math.max(length - suffixLength, 1)
    val end = Math.max(length - 1, 0)
    var nfeatures = features

    if (start == end) {
      val debug = 0
    }

    for (i <- start to end) {

      val suffix = token.substring(i)
      if (suffix.matches("\\p{L}+")) {
        nfeatures = nfeatures :+ (featLabel + "=" + suffix)
      }

    }

    nfeatures

  }

  def prefixes(token: String, featLabel: String, prefixLength: Int): Seq[String] = {
    val features = Seq[String]()
    prefixes(features, token, featLabel, prefixLength)
  }

  def prefixes(features: Seq[String], token: String, featLabel: String, suffixLength: Int): Seq[String] = {
    val length = token.length
    val start = 1
    val end = Math.min(length - 1, suffixLength)
    var nfeatures = features

    for (p <- start to end) {
      val prefix = token.substring(0, p)
      if (prefix.matches("\\p{L}+")) {
        nfeatures = nfeatures :+ (featLabel + "=" + prefix)
      }
    }

    nfeatures
  }
}

//////////////////////////////////////////////////////////////////////////
//</editor-fold>
//////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Greeting Start/End">

object Phrasels{

  lazy val mapPhrasels = Dictionary.loadTRPhrasels()
  lazy val mapPhraselsTypes = Dictionary.loadTRPhraselsTypes()

  def turkishStart(sentence:String):Seq[String]={
    val func = (sentence:String,term:String)=>{
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.startsWith(term)
    }

    turkishPhrasel(sentence,"PHRASEL-START=", func)

  }

  def turkishEnd(sentence:String):Seq[String]={
    val func = (sentence:String,term:String)=>{
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.endsWith(term)
    }

    turkishPhrasel(sentence,"PHRASEL-END=", func)

  }

  def turkishContains(sentence:String):Seq[String]={
    val func = (sentence:String,term:String)=>{
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.contains(term)
    }

    turkishPhrasel(sentence,"PHRASEL-CONTAINS=", func)

  }



  def turkishPhrasel(sentence:String,prefix:String, func:(String,String)=>Boolean) : Seq[String]={

    val phraseStart = prefix+mapPhrasels.exists{case(term,category)=>{
      func(sentence, term)
    }}

    val phraseTypeStarts = mapPhraselsTypes.filterKeys(term=>{
      func(sentence,term)
    }).map(termCategory=>{prefix+termCategory._2})
      .toSeq

    phraseTypeStarts :+ phraseStart
  }


}

//</editor-fold>
///////////////////////////////////////////////////////////