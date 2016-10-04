package org.apache.spark.ml

import java.io.PrintWriter
import java.util.Locale


import language.model.PositionalModel
import language.morphology.AnalyzerImp
import options._
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute, NumericAttribute}

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
    * @return POSTagger Features
    */
  def tokenLabelFeatures(sequence: Seq[(String, String)]): Seq[(Int, Int, String, Seq[String])] = {
    val pid = 0
    val analyzer = ParamsMorphology.analyzer
    sequence.zipWithIndex.map { case ((token, label), indice) => {
      val features =
        TokenShape.shapes(token) ++
          TokenShape.emoticons(token) ++
          TokenShape.suffixes(token, "SUFFIX", ParamsTokenChar.maxSuffixLength) ++
          TokenShape.prefixes(token, "PREFIX", ParamsTokenChar.maxPrefixLength) ++
          TokenStem.stem(analyzer, token) ++
          TokenStem.stemSuffix(analyzer, token, ParamsTokenChar.maxSuffixLength) ++
          TokenStem.stemPrefix(analyzer, token, ParamsTokenChar.maxPrefixLength) ++
          TokenShape.self(token) ++
          TokenShape.symbols(token)
      (pid, indice, token, features)
    }
    }
  }

  def morphTagFeatures(): Seq[Seq[String]] => Seq[String] = {
    tagSequence: Seq[Seq[String]] => {

      //To DO: Filter necessary pos tags
      tagSequence.flatten
    }
  }

  def morphTagRatios(): Seq[Seq[String]] => Seq[Double] = {
    tagSequence: Seq[Seq[String]] => {
      ///TODO: Count number of tags per word
      val tagSum = tagSequence.foldLeft[Double](0.0)((sum, tags)=>{
        sum + tags.length
      })

      val tagAverage = tagSum / tagSequence.length

      Seq(tagAverage)

    }
  }

  def morphTagRatioMeta(): Array[Attribute] = {
    val defaultAttr = NumericAttribute.defaultAttr
    Array(
      "TagCountPerWord"
    ).map(defaultAttr.withName)
  }

  def sentenceTokenShapeFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.zipWithIndex.flatMap { case (token, indice) => {
        TokenShape.shapes(token)
      }
      }
    }
  }

  def sentenceTokenSuffixPrefixFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.flatMap(token => {
        TokenShape.suffixes(token, "SUFFIX", ParamsTokenChar.maxSuffixLength) ++
          TokenShape.prefixes(token, "PREFIX", ParamsTokenChar.maxPrefixLength)

      })
    }
  }

  def sentenceTokenEmoticonFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.zipWithIndex.flatMap { case (token, indice) => {
        TokenShape.emoticons(token)
      }
      }
    }
  }


  def sentenceTokenSymbolFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.zipWithIndex.flatMap { case (token, indice) => {
        TokenShape.symbols(token)
      }
      }
    }
  }

  def sentenceTokenSelfFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.map { token => {
        TokenShape.lowercase(token)
      }
      }
    }
  }

  def sentenceTokenLowercaseFeatures(): Seq[String] => Seq[String] = {
    tokens: Seq[String] => {
      tokens.map { token => {
        TokenShape.lowercase(token)
      }
      }
    }
  }

  def sentenceStemFeatures(): Seq[String] => Seq[String] = {
    val analyzer = ParamsMorphology.analyzer
    tokens: Seq[String] => {
      tokens.flatMap(token => {
        //Stem of the word without morphological analyzer
        TokenStem.stem(analyzer, token) ++
          TokenStem.stemSuffix(analyzer, token, ParamsTokenChar.maxSuffixLength) ++
          TokenStem.stemPrefix(analyzer, token, ParamsTokenChar.maxPrefixLength)
      })
    }
  }


  def sentenceTokenGramFeatures(): Seq[String] => Seq[String] = {
    val max = ParamsTokenGram.maxTokenGramLength
    val min = ParamsTokenGram.minTokenGramLength

    tokens: Seq[String] => {
      ngramsString(tokens, min, max)
    }
  }

  def sentencePOSGramFeatures(): Seq[String] => Seq[String] = {
    val max = ParamsTokenGram.maxPOSGramLength
    val min = ParamsTokenGram.minPOSGramLength

    poses: Seq[String] => {
      ngramsString(poses, min, max)
    }
  }

  def textRepeatedPuncFeatures(): String => Seq[String] = {
    val puncpattern = new Regex("([\\p{Punct}\\@\\'\\,\\&])(\\1)+")
    sentence: String => {
      puncpattern.findAllIn(sentence).toSeq
    }
  }

  def textShapeFeatures(): String => Seq[String] = {

    text: String => {

      TextShape.digitNumerStarts(text) ++
        TextShape.letterNumberStarts(text) ++
        TextShape.numeratorStarts(text) ++
        TextShape.indentationStarts(text)

    }
  }

  def textCharGramFeatures(): String => Seq[String] = {
    val max = ParamsSentenceChar.maxCharGramLength
    val min = ParamsSentenceChar.minCharGramLength

    sentence: String => {
      val chars = sentence.toCharArray.toSeq
      ngramsChar(chars, min, max)

    }
  }

  def turkishPhrasels(): String => Seq[String] = {
    val fun: String => Seq[String] = {
      var features = Seq[String]()
      sentence: String => {

        features = features ++ Phrasels.turkishStart(sentence)
        features = features ++ Phrasels.turkishEnd(sentence)
        features = features ++ Phrasels.turkishContains(sentence)

        features
      }
    }
    fun
  }

  def turkishMorphology(): Seq[String] => Seq[String] = {
    val func: Seq[String] => Seq[String] = {
      var features = Seq[String]()
      morphs: Seq[String] => {
        features = features ++ Morphology.stems(morphs)
        features
      }
    }

    func
  }

  /**
    * input tokens
    *
    * @return
    */
  def turkishFunctionWordPosTags(): Seq[String] => Seq[String] = {
    val func: Seq[String] => Seq[String] = {
      var features = Seq[String]()
      tokens: Seq[String] => {
        features = features ++ FunctionWords.topFunctionWordPosTags(tokens, ParamsFunctionWords.topCommonFunctionPOSTag)
        features
      }
    }

    func
  }

  def turkishFunctionWordCounts():Seq[String]=>Seq[Double]={

    tokens:Seq[String]=>{
      FunctionWords.allFunctionWordCounts(tokens)
    }
  }

  def turkishFunctionWordCountsMeta():Array[Attribute]={
    val defaultAttr = NumericAttribute.defaultAttr
    FunctionWords.functionWordsMap.map(pair=>pair._1)
      .toArray
      .map(defaultAttr.withName)
  }

  def turkishFunctionWordPosCounts():Seq[String]=>Seq[Double]={
    val func:Seq[String]=>Seq[Double]={
      tokens:Seq[String]=>{
        FunctionWords.commonFunctionPOSTagCounts(tokens)
      }
    }
    func
  }

  def turkishFunctionWordPosCountsMeta(): Array[Attribute] = {
    val defaultAttr = NumericAttribute.defaultAttr
    FunctionWords.distinctCommonFunctionPosTags
      .toArray
      .map(defaultAttr.withName)
  }

  /**
    *
    * input: sentences, tokens
    * output: rate of function words per token, maximum by sentence
    */
  def functionWordRates(): (Seq[String], Seq[String]) => Seq[Double] = {
    val func: (Seq[String], Seq[String]) => Seq[Double] = {
      (sentences: Seq[String], tokens: Seq[String]) => {
        val text = sentences.mkString(" ")
        val numSentences = sentences.size
        val numTokens = tokens.size
        val funcCounts = FunctionWords.functionWords(text)
        val avgLength = FunctionWords.lengthFunctionWords(funcCounts)
        val totalCount = FunctionWords.countFunctionWords(funcCounts)
        val distinctCount = FunctionWords.countDistinctFunctionWords(funcCounts)

        val tokenDenom = if (numTokens == 0) 1.0 else numTokens
        val sentenceDenom = if (numSentences == 0) 1.0 else numSentences
        val avgRatePerToken = totalCount / tokenDenom
        val avgRatePerSentence = totalCount / sentenceDenom
        Seq[Double](avgLength, totalCount, distinctCount, avgRatePerToken, avgRatePerSentence)
      }
    }

    func
  }

  def functionWordRatesMeta(): Array[Attribute] = {
    val defaultAttr = NumericAttribute.defaultAttr
    Array(
      "AvgFuncWordRate", "TotalFuncWordCount", "DistinctFuncWordCount", "AvgFuncWordRatePerToken",
      "AvgFuncWordRatePerSen"
    ).map(defaultAttr.withName)
  }

  def textLengthRates(): (Seq[String], Seq[String], Seq[String]) => Seq[Double] = {
    val func: (Seq[String], Seq[String], Seq[String]) => Seq[Double] = {
      (paragraphs: Seq[String], sentences: Seq[String], tokens: Seq[String]) => {

        val text = paragraphs.mkString("\n")
        val textLength = text.length.toDouble
        val wordLength = Lengths.totalWordLength(tokens)
        val puncLength = Lengths.totalPuncLength(tokens)
        val digitLength = Lengths.totalDigitLength(tokens)

        val avgSentenceLength = Lengths.averageSentenceLength(sentences)
        val avgTokenLength = Lengths.averageTokenLength(tokens)
        val avgParagraphLength = Lengths.averageParagraphLength(paragraphs)
        val avgWordLength = Lengths.averageWordLength(tokens)
        val avgDigitLength = Lengths.averageDigitLength(tokens)
        val avgPuncLength = Lengths.averagePunctLength(tokens)
        val avgSpaceLength = Lengths.averageSpaceLength(sentences)
        val cntAboveTokenLength = Lengths.countAboveLength(tokens, ParamsLength.aboveTokenLength)
        val cntBelowTokenLength = Lengths.countBelowLength(tokens, ParamsLength.belowTokenLength)
        val minSentenceLength = Lengths.minimumSentenceLength(sentences)
        val maxSentenceLength = Lengths.maximumSentenceLength(sentences)
        val minTokenLength = Lengths.minimumTokenLength(tokens)
        val maxTokenLength = Lengths.maximumTokenLength(tokens)
        val textLengthDenom = if (textLength == 0) 1.0 else textLength
        val rateWordLength = wordLength / textLengthDenom
        val ratePuncLength = puncLength / textLengthDenom
        val rateDigitLength = digitLength / textLengthDenom

        Seq(
          textLength,
          wordLength,
          puncLength,
          digitLength,
          avgSentenceLength,
          avgTokenLength,
          avgParagraphLength,
          avgWordLength,
          avgDigitLength,
          avgPuncLength,
          avgSpaceLength,
          cntAboveTokenLength,
          cntBelowTokenLength,
          minSentenceLength,
          maxSentenceLength,
          minTokenLength,
          maxTokenLength,
          rateWordLength,
          ratePuncLength,
          rateDigitLength)
      }
    }

    func
  }

  def textLengthRatesMeta(): Array[Attribute] = {
    val defaultAttr = NumericAttribute.defaultAttr
    Array(
      "TextLength",
      "WordLength",
      "PuncLength",
      "DigitLength",
      "AvgSentenceLength",
      "AvgTokenLength",
      "AvgParagraphLength",
      "AvgWordLength",
      "AvgDigitLength",
      "AvgPuncLength",
      "AvgSpaceLength",
      "CountAboveCertainTokLen",
      "CountBelowCertainTokLen",
      "MinSentenceLength",
      "MaxSentenceLength",
      "MinTokenLength",
      "MaxTokenLength",
      "rateWordLength",
      "ratePuncLength",
      "rateDigitLength"
    ).map(defaultAttr.withName)
  }

  def vocabularyRichness(): Seq[String] => Seq[Double] = {
    val func: Seq[String] => Seq[Double] = {
      tokens: Seq[String] => {
        VocabularyRichness.vocabularyRichness(tokens)
      }
    }

    func
  }

  def vocabularyRichnessMeta(prefix: String): Array[Attribute] = {
    VocabularyRichness.vocabularyRichnessMeta(prefix)
  }


  def positionalLMScore(): Seq[String] => Seq[Double] = {
    //val func:Seq[String]=>Seq[Double] ={
    sentences: Seq[String] => {
      LanguageModels.positionalFeatureRates(sentences)
    }
  }


  def positionalLMScoreMeta(): Array[Attribute] = {
    LanguageModels.positionalFeatureRatesMeta()
  }


  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("===Length and Percentage Based Features===\n")

    printWriter.append("Text Length\n")
    printWriter.append("Word Length\n")
    printWriter.append("Total Punctuation Lengths\n")
    printWriter.append("Total Digit Lengths\n")
    printWriter.append("Average Sentence Length\n")
    printWriter.append("Average Token Length\n")
    printWriter.append("Average Paragraph Length\n")
    printWriter.append("Average Word Length\n")
    printWriter.append("Average Digit Length\n")
    printWriter.append("Average Punctuation Length\n")
    printWriter.append("Average Space Length\n")
    printWriter.append("Count above length\n")
    printWriter.append("Count below length\n")
    printWriter.append("Minimum Sentence length\n")
    printWriter.append("Maximum Sentence length\n")
    printWriter.append("Minimum Token length\n")
    printWriter.append("Maximum Token length\n")
    printWriter.append("Ratio of word length to text length\n")
    printWriter.append("Ratio of punctuation length to text length\n")
    printWriter.append("Ratio of digit length to text length\n")

    printWriter.append("===Function Word Rates===\n")
    printWriter.append("Average Length of Function Words\n")
    printWriter.append("Count of Function Words\n")
    printWriter.append("Distinct count of Function Words\n")
    printWriter.append("Ratio of function words to tokens\n")
    printWriter.append("Ratio of function words to sentences\n")

    printWriter.append("===NGram Features===\n")
    printWriter.append("N-Grams of text/token/pos-tags\n")


    Phrasels.explain(printWriter)
    FunctionWords.expain(printWriter)
    Morphology.explain(printWriter)
    TokenShape.explain(printWriter)
    TextShape.explain(printWriter)
    VocabularyRichness.explain(printWriter)
    LanguageModels.explain(printWriter)


  }


}


//////////////////////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Stem/Suffix Features">

object TokenStem {

  def stemmingNoun(analyzerImp: AnalyzerImp, tokens: Array[String]): Seq[String] = {
    analyzerImp.stemBy(tokens, "Noun")
      .map(stem => stem.toLowerCase(Parameters.crrLocale))
      .toArray[String]
  }

  def stemmingSnowball(analyzerImp: AnalyzerImp, token: String): String = {
    analyzerImp.stemSnowball(token)
  }

  def stemming(analyzerImp: AnalyzerImp, token: String): String = {
    analyzerImp.stem(token)
  }

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

  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("====Token Stem Features====\n")

    printWriter.append("Stem and suffix of the token\n")
    printWriter.append("Suffix and prefix grams of the stem and the suffix\n")


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


  def lowercase(token: String): String = {
    token.toLowerCase(locale)
  }

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


  def symbols(token: String): Seq[String] = {
    var features = Seq[String]()

    if (token.matches(".*[-].*")) {
      features = features :+ "SYM-HYPEN"
    }

    if (token.matches(".*[/].*")) {
      features = features :+ "SYM-BACKSLASH"
    }

    if (token.matches(".*[\\[].*")) {
      features = features :+ "SYM-OPENSQUARE"
    }

    if (token.matches(".*[\\]].*")) {
      features = features :+ "SYM-CLOSESQUARE"
    }

    if (token.matches(".*[:].*")) {
      features = features :+ "SYM-COLON"
    }

    if (token.matches(".*[;].*")) {
      features = features :+ "SYM-SEMICOLON"
    }

    if (token.matches(".*[%].*")) {
      features = features :+ "SYM-PERCENTAGE"
    }

    if (token.matches(".*[\\(].*")) {
      features = features :+ "SYM-OPENPAREN"
    }


    if (token.matches(".*[\\)].*")) {
      features = features :+ "SYM-CLOSEPAREN"
    }

    if (token.matches(".*[\\,].*")) {
      features = features :+ "SYM-COMMA"
    }


    if (token.matches(".*[\\.].*")) {
      features = features :+ "SYM-DOT"
    }


    if (token.matches(".*[\\'].*")) {
      features = features :+ "SYM-APOSTROPHE"
    }

    if (token.matches(".*[\"].*")) {
      features = features :+ "SYM-QUOTA"
    }

    if (token.matches(".*[\\*].*")) {
      features = features :+ "SYM-STAR"
    }

    if (token.matches(".*[\\=].*")) {
      features = features :+ "SYM-EQUAL"
    }


    if (token.matches(".*[\\+].*")) {
      features = features :+ "SYM-PLUS"
    }

    if (token.matches("\"((?=[MDCLXVI])((M{0,3})((C[DM])|(D?C{0,3}))?((X[LC])|(L?XX{0,2})|L)?((I[VX])|(V?(II{0,2}))|V)?))\"")) {
      features = features :+ "SYM-ROMAN"
    }

    features
  }

  def explain(printWriter: PrintWriter): Unit = {

    printWriter.append("===Token Shape Features===\n")

    printWriter.append("Token in lowercase\n")
    printWriter.append("Shapes of token with starting/ending and containing characters (uppercase/lowercase/digit)\n")
    printWriter.append("Emoticon on/off feature\n")
    printWriter.append("Suffix and prefix grams of the token\n")
    printWriter.append("Tokens Containing Symbol Features\n")


  }
}

//////////////////////////////////////////////////////////////////////////
//</editor-fold>
//////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Greeting Start/End">

object Phrasels {

  lazy val mapPhrasels = Dictionary.loadTRPhrasels()
  lazy val mapPhraselsTypes = Dictionary.loadTRPhraselsTypes()

  def turkishStart(sentence: String): Seq[String] = {
    val func = (sentence: String, term: String) => {
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.startsWith(term)
    }

    turkishPhrasel(sentence, "PHRASEL-START=", func)

  }

  def turkishEnd(sentence: String): Seq[String] = {
    val func = (sentence: String, term: String) => {
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.endsWith(term)
    }

    turkishPhrasel(sentence, "PHRASEL-END=", func)

  }

  def turkishContains(sentence: String): Seq[String] = {
    val func = (sentence: String, term: String) => {
      val lowercase = sentence.toLowerCase(Parameters.trLocale)
      lowercase.contains(term)
    }

    turkishPhrasel(sentence, "PHRASEL-CONTAINS=", func)

  }


  protected def turkishPhrasel(sentence: String, prefix: String, func: (String, String) => Boolean): Seq[String] = {

    val phraseStart = prefix + mapPhrasels.exists { case (term, category) => {
      func(sentence, term)
    }
    }

    val phraseTypeStarts = mapPhraselsTypes.filterKeys(term => {
      func(sentence, term)
    }).map(termCategory => {
      prefix + termCategory._2
    })
      .toSeq

    phraseTypeStarts :+ phraseStart
  }

  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("===Phrase Features===\n")

    printWriter.append("Does Sentence start with a phrase (on/off) feature\n")
    printWriter.append("Does Sentence contains a phrase (on/off) feature\n")
    printWriter.append("Does Sentence ends with a phrase (on/off) feature\n")
    printWriter.append("Category of the phrase start\n")
    printWriter.append("Category of the phrase containing\n")
    printWriter.append("Category of the phrase ending\n")


  }


}

//</editor-fold>
///////////////////////////////////////////////////////////

object FunctionWords {

  val functionWordsMap = Dictionary.loadFunctionWords()
  val commonFunctionPosTags = Dictionary.loadCommonPosTags()
  val distinctCommonFunctionPosTags = commonFunctionPosTags.toSeq
    .map { case (word, tag) => tag }.distinct

  def functionWords(text: String): Seq[(String, Int)] = {
    functionWordsMap.keySet.map(functionWord => {
      (functionWord, functionWord.r.findAllIn(text)
        .size)
    }).toSeq
  }

  def countFunctionWords(funcWords: Seq[(String, Int)]): Double = {
    funcWords.map(_._2).sum.toDouble
  }

  def countDistinctFunctionWords(funcWords: Seq[(String, Int)]): Double = {
    funcWords.map(_._1).distinct.length.toDouble
  }


  def lengthFunctionWords(funcWords: Seq[(String, Int)]): Double = {
    val occurs = funcWords.filter { case (funcWord, count) => count > 0 }
    val totalLength = occurs.map(_._1.length).sum
    val occursDenom = if (occurs.size == 0) 1.0 else occurs.size

    totalLength / occursDenom
  }


  def allFunctionWordCounts(tokens: Seq[String]): Seq[Double] = {
    val text = tokens.mkString(" ")
    functionWordsMap.map(pair => {
      val functionWord = pair._1
      functionWord.r.findAllIn(text)
        .size
        .toDouble
    }).toSeq
  }

  def commonFunctionPOSTagCounts(tokens: Seq[String]): Seq[Double] = {
    val text = tokens.mkString(" ")
    var map = distinctCommonFunctionPosTags.map(tag => (tag, 0.0)).toMap

    commonFunctionPosTags.foreach(pair => {
      val functionWord = pair._1
      val posTag = pair._2
      val count = functionWord.r.findAllIn(text)
        .size
        .toDouble
      map = map.updated(posTag, map.getOrElse(posTag, 0.0) + count)
    })

    map.map(pair => pair._2).toSeq
  }

  def topFunctionWordPosTags(tokens: Seq[String], top: Int): Seq[String] = {
    val text = tokens.mkString(" ")
    val counts = commonFunctionPosTags.map(pair => {
      val functionWord = pair._1
      val posTag = pair._2
      ("COMMON-FUNCT-POS=" + posTag, functionWord.r.findAllIn(text).size)
    }).toSeq.sortBy(_._2)

    counts.take(top).map(_._1)
  }

  def expain(printWriter: PrintWriter): Unit = {

    printWriter.append("===Function Word Features===\n")
    printWriter.append("Count of Function Words\n")
    printWriter.append("Count of Distinct Function Words\n")
    printWriter.append("Average Function Word Length\n")
    printWriter.append("Common Function Word Counts\n")
    printWriter.append("Common Function Word POS Tag Counts\n")
    printWriter.append("Top Common Function Word POS Tags\n")

  }
}


//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Length/Count Features">
object Lengths {


  def averageTokenLength(tokens: Seq[String]): Double = {
    averageLength(tokens)
  }

  def averageSentenceLength(sentences: Seq[String]): Double = {
    averageLength(sentences)
  }

  def averageParagraphLength(paragraphs: Seq[String]): Double = {
    averageLength(paragraphs)
  }


  def totalWordLength(tokens: Seq[String]): Double = {
    totalPatternLength(tokens, "\\p{L}+")
  }

  def totalPuncLength(tokens: Seq[String]): Double = {
    totalPatternLength(tokens, "\\p{Punct}+")
  }

  def totalDigitLength(tokens: Seq[String]): Double = {
    totalPatternLength(tokens, "\\d+")
  }

  def averageWordLength(tokens: Seq[String]): Double = {
    averagePatternLength(tokens, "\\p{L}+")
  }


  def averagePunctLength(tokens: Seq[String]): Double = {
    averagePatternLength(tokens, "\\p{Punct}+")
  }

  def averageDigitLength(tokens: Seq[String]): Double = {
    averagePatternLength(tokens, "(\\d+[\\.\\,]?)+")
  }

  def averageSpaceLength(sentences: Seq[String]): Double = {
    val patterns = find(sentences, "[\\s\\t\\n]+")
    averageLength(patterns)
  }


  def minimumTokenLength(tokens: Seq[String]): Double = {
    minimumLength(tokens)
  }

  def maximumTokenLength(tokens: Seq[String]): Double = {
    maximumLength(tokens)
  }

  def minimumSentenceLength(sentences: Seq[String]): Double = {
    minimumLength(sentences)
  }

  def maximumSentenceLength(sentences: Seq[String]): Double = {
    maximumLength(sentences)
  }


  def minimumSentenceLengthByToken(sentence: Seq[Seq[String]]): Double = {
    minimumSize(sentence)
  }

  def maximumSentenceLengthByToken(sentence: Seq[Seq[String]]): Double = {
    maximumSize(sentence)
  }

  def countBelowLength(tokens: Seq[String], len: Int = 5): Double = {
    tokens.map(token => token.length)
      .filter(length => length < len)
      .size.toDouble
  }

  def countAboveLength(tokens: Seq[String], len: Int = 10): Double = {
    tokens.map(token => token.length)
      .filter(length => length < len)
      .size.toDouble
  }

  def countPattern(sentences: Seq[String], pattern: String): Double = {
    find(sentences, pattern).size.toDouble
  }

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Utility ">

  def totalPatternLength(tokens: Seq[String], pattern: String): Double = {
    val words = tokens.filter(token => {
      token.matches(pattern)
    })

    words.map(word => word.length).sum.toDouble
  }

  def averagePatternLength(tokens: Seq[String], pattern: String): Double = {
    val words = tokens.filter(token => {
      token.matches(pattern)
    })
    val denom = if (words.length == 0) 1.0 else words.length
    val sum = words.map(word => word.length).sum.toDouble
    sum / denom
  }

  def averageLength(sequence: Seq[String]): Double = {
    val denom = if (sequence.size == 0) 1.0 else sequence.size
    sequence.map(token => token.length).sum.toDouble / denom
  }

  def minimumLength(tokens: Seq[String]): Double = {
    tokens.map(_.length).min
  }

  def minimumSize(tokens: Seq[Seq[String]]): Double = {
    tokens.map(_.size).min.toDouble
  }

  def maximumLength(tokens: Seq[String]): Double = {
    tokens.map(_.length).max.toDouble
  }

  def maximumSize(tokens: Seq[Seq[String]]): Double = {
    tokens.map(_.size).max.toDouble
  }

  def find(sentences: Seq[String], pattern: String): Seq[String] = {
    val regex = new Regex(pattern)
    sentences.flatMap(sentence => {
      regex.findAllIn(sentence)
    })
  }

  def explain(printWriter: PrintWriter): Unit = {

  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

}

//</editor-fold>
///////////////////////////////////////////////////////////

object LanguageModels {
  val positionalModel = new PositionalModel(ParamsLanguageModel.lmchunkNum)

  def positionalFeatureRates(sentences: Seq[String]): Seq[Double] = {
    val seq = positionalModel.scoreRegular(sentences)
      .flatten
      .toSeq

    seq
  }

  def positionalFeatureRatesMeta(): Array[Attribute] = {
    val modelSize = positionalModel.models.length
    val numerico = NumericAttribute.defaultAttr
    Range(0, ParamsLanguageModel.lmchunkNum)
      .toArray.flatMap(i => {
      Range(0, modelSize).toArray.map(m => {
        "lmposition_" + i + "_m_" + m
      })
    }).map(numerico.withName)
  }

  def explain(printWriter: PrintWriter): Unit = {
    if (ParamsFeatures.lmRegularModel || ParamsFeatures.lmTransModel) {
      printWriter.append("===Positional Language Model Vector===\n")
      printWriter.append("Language Model for specific parts of text\n")
    }
  }
}

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Morphological Features">

object Morphology {

  def stems(morphs: Seq[String]): Seq[String] = {
    morphs.map(morph => {
      "STEM=" + morph.split("\\+")(0)
    })
  }

  def morphTags(morphTagsAll: Seq[String]): Seq[String] = {
    morphTagsAll
  }

  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("===MORPHOLOGICAL ANALYSIS FEATURES===\n")
    printWriter.append("All morphological analysis tags in document\n")
    printWriter.append("Morphological analysis stem\n")

  }
}

//</editor-fold>
///////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Document Shape Characteristics">
object TextShape {


  protected def patternize(text: String, label: String, regex: Regex): Seq[String] = {
    regex.findAllIn(text).map(matching => {
      label
    }).toSeq
  }


  def digitNumerStarts(text: String): Seq[String] = {
    val regex = "\\d+\\s?[\\)\\]\\.]?".r
    patternize(text, "DIGIT-NUMER-START", regex)
  }

  def letterNumberStarts(text: String): Seq[String] = {
    val regex = "\\p{L}+\\s?[\\)\\]\\.]?".r
    patternize(text, "LETTER-NUMER-START", regex)
  }

  def numeratorStarts(text: String): Seq[String] = {

    val regex = "[\\p{L}\\d]+\\s?[\\)\\]\\.]?".r
    patternize(text, "NUMER-START", regex)

  }

  def indentationStarts(text: String): Seq[String] = {

    val regexTabLetter = "\t+\\p{L}".r
    val regexTabDigit = "\t+\\d+".r

    patternize(text, "TAB-LETTER-START", regexTabLetter) ++
      patternize(text, "TAB-DIGIT-START", regexTabDigit)
  }

  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("===TEXT SHAPE FEATURES===\n")
    printWriter.append("Digit/Letter Numerators\n")
    printWriter.append("TAB Digit/Letter Indentations\n")

  }

}

//</editor-fold>
///////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////
//<editor-fold defaultstate="collapsed" desc="Vocabulary Richness Characteristics">
object VocabularyRichness {

  def explain(printWriter: PrintWriter): Unit = {
    printWriter.append("===VOCABULARY RICHNESS/COMPLEXITY FEATURES===\n")
    printWriter.append("Guirads'R Measure\n")
    printWriter.append("Herdans'C Measure\n")
    printWriter.append("Rubets'K Measure\n")
    printWriter.append("Maas'A Measure\n")
    printWriter.append("Dugast's U Measure\n")
    printWriter.append("Janenkov and Neistoj Measure\n")
    printWriter.append("Brunet's V Measure\n")
    printWriter.append("Happax Measure\n")
    printWriter.append("Lemma Diversity Vector (For each lemma, the number different words mapped to it) \n")
    printWriter.append("Average frequency scores for tokens \n")
  }

  def vocabularyUniqueness(tokens: Seq[String]): Seq[String] = {
    //words that occur only once
    //used also in happax : number of words that appear only one time and are only used by the current author

    //Number of distinct/unique tokens
    //Each tokens frequency scores
    //Average frequency score ??
    tokens
  }

  def vocabularyRichness(tokens: Seq[String]): Seq[Double] = {
    val tokenCounts = tokenCount(tokens)
    val V = tokenCounts.size.toDouble
    val N = tokens.length.toDouble
    val happaxDis = happaxDislegomena(tokenCounts).toDouble

    val scores = Seq[Double](
      guiradsR(V, N),
      herdansC(V, N),
      rubetsK(V, N),
      maasA(V, N),
      dugastU(V, N),
      janenkovNeistoj(V, N),
      brunetsW(V, N),
      happaxMeasure(V, happaxDis)
    )

    scores
  }

  def vocabularyRichnessMeta(prefix: String): Array[Attribute] = {
    val numerico = NumericAttribute.defaultAttr
    Array(
      "guiradsR",
      "herdansC",
      "rubetsK",
      "massA",
      "dugastU",
      "janenkovNeistoj",
      "brunetsW",
      "happaxMeasure"
    ).map(attr => prefix + "-" + attr).map(numerico.withName)
  }

  def lemmaDiversity(lemmas: Seq[String], tokens: Seq[String]): Seq[(String, Double)] = {
    //for each lemma, the number different words mapped to it

    require(lemmas.length == tokens.length, "Lemma and token length must be the same!")

    var lemmaWordMap = Map[String, Seq[String]]()

    for (i <- 0 until lemmas.length) {
      val lemma = lemmas(i)
      val token = tokens(i)
      val seq = lemmaWordMap.getOrElse(lemma, Seq[String]())
      if (!seq.contains(token)) {
        lemmaWordMap = lemmaWordMap.updated(lemma, seq :+ token)
      }

    }

    lemmaWordMap.map { case (lemma, seq) => {
      (lemma, seq.length.toDouble)
    }
    }
      .toArray[(String, Double)]
  }

  protected def tokenCount(tokens: Seq[String]): Map[String, Int] = {
    tokens.groupBy(token => token).mapValues(seq => seq.length)
  }


  protected def happaxDislegomena(counts: Map[String, Int], count: Int = 2): Long = {
    counts.count(pair => pair._2 == count)
  }

  protected def guiradsR(V: Double, N: Double): Double = {
    V / scala.math.sqrt(N)
  }

  protected def herdansC(V: Double, N: Double): Double = {
    scala.math.log10(V) / scala.math.log10(N)
  }

  protected def rubetsK(V: Double, N: Double): Double = {
    val value = scala.math.log10(V) / scala.math.log10(scala.math.log10(N))
    if (value.isInfinity) 0
    else if (value < 0.0) 0d
    else value
  }

  protected def maasA(V: Double, N: Double): Double = {
    val numerator = math.log10(N) / math.log10(V)
    val ratio = numerator / math.pow(math.log10(N), 2)
    math.sqrt(ratio)
  }

  protected def dugastU(V: Double, N: Double): Double = {
    val numerator = math.pow(math.log10(N), 2)
    val denom = math.log10(N) / math.log10(V)
    numerator / denom
  }

  protected def janenkovNeistoj(V: Double, N: Double): Double = {
    val denom = math.pow(V, 2) * math.log10(N)
    1 / denom
  }

  protected def brunetsW(V: Double, N: Double): Double = {
    (V - 0.172) * math.log(N)
  }

  protected def happaxMeasure(V: Double, X: Double): Double = {
    X / V
  }

  def main(args: Array[String]) {
    val tokens = Seq("AA", "AAA", "CC", "CCC", "AA", "AKK")
    val lemmas = Seq("A", "A", "C", "C", "A", "A")

    println(lemmaDiversity(lemmas, tokens))
  }

}


//</editor-fold>
///////////////////////////////////////////////////////////
