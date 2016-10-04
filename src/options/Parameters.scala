package options

import java.io.PrintWriter
import java.util.Locale

import data.dataset.XMLParser
import language.morphology._
import language.postagger.{PosTaggerCRF, PosTaggerImp}
import language.tokenization.{MyTokenizer, TokenizerImp}
import options.Resources.{DatasetResources, MorphologyResources}
import org.apache.spark.ml.pipes.PipesWord2VecMerging

/**
  * Created by wolf on 17.04.2016.
  */
object Parameters {

  val trLocale = new Locale("tr")
  val enLocale = new Locale("en")
  var crrLocale = new Locale("tr")
  var turkishText = true
  var englishText = false


  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====Global Parameters====\n")
    printWriter.append("Turkish Text: " + turkishText.toString + "\n")
    printWriter.append("English Text: " + englishText.toString + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"Global Parameters\">\n")
    ParamsWriter.appendAsXML(printWriter, "Turkish Documents", "turkishText", turkishText.toString)
    ParamsWriter.appendAsXML(printWriter, "English Documents", "englishText", englishText.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    turkishText = paramMap.getOrElse("turkishText", turkishText.toString).toBoolean
    englishText = paramMap.getOrElse("englishText", englishText.toString).toBoolean
  }

  override def hashCode(): Int = {
    var result: Int = 1
    result = 7 * result + turkishText.hashCode()
    result = 7 * result + englishText.hashCode()

    result
  }

}

object ParamsSentenceChar {

  val name = "Sentence Char Gram Lengths"
  var maxCharGramLength = 8
  var minCharGramLength = 3

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====" + name + "====\n")
    printWriter.append("Maximum Char Gram Length: " + maxCharGramLength + "\n")
    printWriter.append("Minimum Char Gram Length: " + minCharGramLength + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Maximum Char Gram Length", "maxCharGramLength", maxCharGramLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Char Gram Length", "minCharGramLength", minCharGramLength.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {

    maxCharGramLength = paramMap.getOrElse("maxCharGramLength", maxCharGramLength.toString).toInt
    minCharGramLength = paramMap.getOrElse("minCharGramLength", minCharGramLength.toString).toInt
  }

  def map(): Map[String, Set[String]] = {

    Map(name -> Set(
      "maxCharGramLength", "minCharGramLength"
    ))

  }


  override def hashCode(): Int = {
    var result = 1
    result = 7 * result + maxCharGramLength
    result = 7 * result + minCharGramLength
    result
  }
}

object ParamsTokenChar {
  val name = "Token Char Gram Lengths"
  var maxSuffixLength = 3
  var minSuffixLength = 1
  var maxPrefixLength = 5
  var minPrefixLength = 3

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====" + name + "====\n")
    printWriter.append("Maximum Suffix Length: " + maxSuffixLength + "\n")
    printWriter.append("Minimum Suffix Length: " + minSuffixLength + "\n")
    printWriter.append("Maximum Prefix Length: " + maxPrefixLength + "\n")
    printWriter.append("Minimum Prefix Length: " + minPrefixLength + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Maximum Suffix Length", "maxSuffixLength", maxSuffixLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Suffix Length", "minSuffixLength", minSuffixLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Prefix Length", "maxPrefixLength", maxPrefixLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Prefix Length", "minPrefixLength", minPrefixLength.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    maxSuffixLength = paramMap.getOrElse("maxSuffixLength", maxSuffixLength.toString).toInt
    minSuffixLength = paramMap.getOrElse("minSuffixLength", minSuffixLength.toString).toInt
    maxPrefixLength = paramMap.getOrElse("maxPrefixLength", maxPrefixLength.toString).toInt
    minPrefixLength = paramMap.getOrElse("minPrefixLength", minPrefixLength.toString).toInt
  }

  def map(): Map[String, Set[String]] = {

    Map(name -> Set(
      "maxSuffixLength", "minSuffixLength", "maxPrefixLength", "minPrefixLength"
    ))

  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + maxSuffixLength
    result = result * 7 + minSuffixLength
    result = result * 7 + maxPrefixLength
    result = result * 7 + minPrefixLength
    result
  }
}

object ParamsTokenGram {

  val name = "Token Gram Lengths"

  var maxTokenGramLength = 3
  var minTokenGramLength = 2

  var maxPOSGramLength = 3
  var minPOSGramLength = 2

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====" + name + "====\n")
    printWriter.append("Maximum Token Gram Length: " + maxTokenGramLength + "\n")
    printWriter.append("Minimum Token Gram Length: " + minTokenGramLength + "\n")
    printWriter.append("Maximum Pos Gram Length: " + maxPOSGramLength + "\n")
    printWriter.append("Minimum Pos Gram Length: " + minPOSGramLength + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Maximum Token Gram Length", "maxTokenGramLength", maxTokenGramLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Token Gram Length", "minTokenGramLength", minTokenGramLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Pos Gram Length", "maxPOSGramLength", maxPOSGramLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Pos Gram Length", "minPOSGramLength", minPOSGramLength.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    maxTokenGramLength = paramMap.getOrElse("maxTokenGramLength", maxTokenGramLength.toString).toInt
    minTokenGramLength = paramMap.getOrElse("minTokenGramLength", minTokenGramLength.toString).toInt

    maxPOSGramLength = paramMap.getOrElse("maxPOSGramLength", maxPOSGramLength.toString).toInt
    minPOSGramLength = paramMap.getOrElse("minPOSGramLength", minPOSGramLength.toString).toInt

  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set(
      "maxTokenGramLength", "minTokenGramLength", "maxPOSGramLength", "minPOSGramLength"
    ))
  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + maxTokenGramLength
    result = result * 7 + minTokenGramLength
    result = result * 7 + maxPOSGramLength
    result = result * 7 + minPOSGramLength

    result

  }
}

object ParamsLength {
  val name = "Token Counts of Above and Below Token Lengths"
  var aboveTokenLength = 5
  var belowTokenLength = 10


  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")
    printWriter.append("Above Token Length: " + aboveTokenLength + "\n")
    printWriter.append("Below Token Length: " + belowTokenLength + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Count of Above Certain Token Length", "aboveTokenLength", aboveTokenLength.toString)
    ParamsWriter.appendAsXML(printWriter, "Count of Below Certain Token Length", "belowTokenLength", belowTokenLength.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    aboveTokenLength = paramMap.getOrElse("aboveTokenLength", aboveTokenLength.toString).toInt
    belowTokenLength = paramMap.getOrElse("belowTokenLength", belowTokenLength.toString).toInt
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set(
      "aboveTokenLength", "belowTokenLength"
    ))
  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + aboveTokenLength
    result = result * 7 + belowTokenLength

    result
  }
}

object ParamsLanguageModel {
  val name = "Language Model Features"
  var lmchunkNum = 5
  var lmAvgOrMax = true

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")

    printWriter.append("Number of Chunks in Language Model (Vector Size): " + lmchunkNum + "\n")
    printWriter.append("Averaging or Taking Max in Vector Combining: " + lmAvgOrMax + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Number of Chunks in Language Model (Vector Size)", "lmchunkNum", lmchunkNum.toString)
    ParamsWriter.appendAsXML(printWriter, "Count of Below Certain Token Length", "lmAvgOrMax", lmAvgOrMax.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    lmchunkNum = paramMap.getOrElse("lmchunkNum", lmchunkNum.toString).toInt
    lmAvgOrMax = paramMap.getOrElse("lmAvgOrMax", lmAvgOrMax.toString).toBoolean
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set("lmchunkNum", "lmAvgOrMax"))
  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + (if (ParamsFeatures.lmRegularModel || ParamsFeatures.lmTransModel) lmchunkNum else 0)
    result = result * 7 + (if ((ParamsFeatures.lmRegularModel || ParamsFeatures.lmTransModel) && lmAvgOrMax) 1 else 0)

    result
  }

}

object ParamsLDAModel {
  val name = "LDAModel Parameters"
  var topicSliceNum = 5
  var topicPOSFilter = Array("NN.*")

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")

    if (ParamsFeatures.topicModel) {
      printWriter.append("Number of Slice in topic Mode: " + topicSliceNum + "\n")
      printWriter.append("Topic Model Building POS tags: " + topicPOSFilter.mkString("|") + "\n")

    }

  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Number of Token Slices for Topic Model", "topicSliceNum", topicSliceNum.toString)
    ParamsWriter.appendAsXML(printWriter, "Topic Model Building POS Tags", "topicPOSFilter", topicPOSFilter.mkString("|"))
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    topicSliceNum = paramMap.getOrElse("topicSliceNum", topicSliceNum.toString).toInt
    topicPOSFilter = paramMap.getOrElse("topicPOSFilter",topicPOSFilter.mkString("|")).split("\\|")
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set("topicSliceNum", "topicPOSFilter"))
  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + (if (ParamsFeatures.topicModel) topicSliceNum else 0)
    result = result * 7 + (if (ParamsFeatures.topicModel) topicPOSFilter.hashCode() else 0)
    result
  }

}

object ParamsFeatures {

  val name = "Features On/Off"

  var textCharNGrams = false
  var textShape = false
  var termHistogramRates = false


  var lengthFeatureRates = false
  var bowTerm = false
  var tokenShape = false
  var tokenSym = false
  var tokenEmoticon = false
  var tokenSuffix = false

  var tokenGram = false
  var tokenStem = false
  var repeatedPuncs = false

  var morphStem = false
  var morphTag = false
  var morphTagGram = false
  var posTag = false
  var posTagGram = false


  var phraseType = false

  var functionWordPosTag = false
  var functionWordRates = false
  var functionWordHistogramRates = false

  var vocabularyRichnessTokens = false
  var spellSuggestedFeatures = false
  var tokenEmbeddings = false

  var lmTransModel = false
  var lmRegularModel = false

  var topicModel = false

  var useWord2Vec = false
  var word2VecType = PipesWord2VecMerging.documentAveraging


  def append(printWriter: PrintWriter): Unit = {

    printWriter.append("====" + name + "====\n")

    if (lengthFeatureRates) printWriter.append("Length feature rates:" + lengthFeatureRates + "\n")
    if (bowTerm) printWriter.append("Token (BOW) Self: " + bowTerm + "\n")
    if (tokenShape) printWriter.append("Token Shape: " + tokenShape + "\n")
    if (tokenSym) printWriter.append("Token Symbol: " + tokenSym + "\n")
    if (tokenEmoticon) printWriter.append("Token Emoticon: " + tokenEmoticon + "\n")
    if (tokenSuffix) printWriter.append("Token Suffix/Prefix: " + tokenSuffix + "\n")

    if (tokenGram) printWriter.append("Token Gram: " + tokenGram + "\n")
    if (tokenEmbeddings) printWriter.append("Token Embeddings: " + tokenEmbeddings + "\n")
    if (tokenStem) printWriter.append("Token Stem: " + tokenStem + "\n")
    if (repeatedPuncs) printWriter.append("Repeated Puncs: " + repeatedPuncs + "\n")

    if (morphStem) printWriter.append("Morph Stem: " + morphStem + "\n")
    if (morphTag) printWriter.append("Morph Tag: " + morphTag + "\n")
    if (posTag) printWriter.append("Pos Tag: " + posTag + "\n")
    if (posTagGram) printWriter.append("Pos Tag Gram: " + posTagGram + "\n")
    if (phraseType) printWriter.append("Phrase type: " + phraseType + "\n")
    if (functionWordPosTag) printWriter.append("Function Word POS Tags: " + functionWordPosTag + "\n")
    if (functionWordRates) printWriter.append("Function Word Rates: " + functionWordRates + "\n")
    if (functionWordHistogramRates) printWriter.append("Function Word Histogram Rates: " + functionWordHistogramRates + "\n")
    if (vocabularyRichnessTokens) printWriter.append("Vocabulary richness: " + vocabularyRichnessTokens + "\n")
    if (spellSuggestedFeatures) printWriter.append("Spell Suggested Tokens: " + spellSuggestedFeatures + "\n")
    if (textShape) printWriter.append("Text Shape (Indentation/Numerator): " + textShape + "\n")

    if (lmTransModel) printWriter.append("Transition Language Model: " + lmTransModel + "\n")
    if (lmRegularModel) printWriter.append("Regular Language Model: " + lmRegularModel + "\n")

    if (topicModel) printWriter.append("Regular Topic Model: " + topicModel + "\n")
    if (useWord2Vec) printWriter.append("Word2Vec Model: " + useWord2Vec + "\n")
    if (useWord2Vec) printWriter.append("Word2Vec Merging Type: " + word2VecType + "\n")


  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Length Feature Rates (Char/Token/Paragraph Length Proportions)", "lengthFeatureRates", lengthFeatureRates.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Self (BOW)", "bowTerm", bowTerm.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Shape (Camelcase, Digitized, etc..)", "tokenShape", tokenShape.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Emoticons (typed emoticons)", "tokenEmoticon", tokenEmoticon.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Suffix/Prefix (certain max/min length)", "tokenSuffix", tokenSuffix.toString)


    ParamsWriter.appendAsXML(printWriter, "Token Symbol (Quaotation, Question,Semicolon etc..)", "tokenSym", tokenSym.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Grams", "tokenGram", tokenGram.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Embeddings", "tokenEmbeddings", tokenEmbeddings.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Stem Features", "tokenStem", tokenStem.toString)
    ParamsWriter.appendAsXML(printWriter, "Repeated Punctuations", "repeatedPuncs", repeatedPuncs.toString)
    ParamsWriter.appendAsXML(printWriter, "Morph Stem Features", "morphStem", morphStem.toString)
    ParamsWriter.appendAsXML(printWriter, "Morph Tag Features (Derived from Morphological Analysis Results)", "morphTag", morphTag.toString)
    ParamsWriter.appendAsXML(printWriter, "Pos Tag Features", "posTag", posTag.toString)
    ParamsWriter.appendAsXML(printWriter, "Pos Tag Grams", "posTagGram", posTagGram.toString)
    ParamsWriter.appendAsXML(printWriter, "Phrase Type Features (Starting/Ending/Containment)", "phraseType", phraseType.toString)
    ParamsWriter.appendAsXML(printWriter, "Function Word and Common Word Phrases", "functionWordPosTag", functionWordPosTag.toString)
    ParamsWriter.appendAsXML(printWriter, "Function Word Rates and Rations", "functionWordRates", functionWordRates.toString)
    ParamsWriter.appendAsXML(printWriter, "Function Word (Length) Histogram Rates", "functionWordHistogramRates", functionWordHistogramRates.toString)
    ParamsWriter.appendAsXML(printWriter, "Vocabulary Richness Features", "vocabularyRichnessTokens", vocabularyRichnessTokens.toString)
    ParamsWriter.appendAsXML(printWriter, "Spelling Suggested Token Features", "spellSuggestedFeatures", spellSuggestedFeatures.toString)
    ParamsWriter.appendAsXML(printWriter, "Text Shape (Indentation/Numerator)", "textShape", textShape.toString)
    ParamsWriter.appendAsXML(printWriter, "Text Char N-Grams (include token spacing)", "textCharNGrams", textCharNGrams.toString)

    ParamsWriter.appendAsXML(printWriter, "Transition Language Model", "lmTransModel", lmTransModel.toString)
    ParamsWriter.appendAsXML(printWriter, "Regular Language Model", "lmRegularModel", lmRegularModel.toString)
    ParamsWriter.appendAsXML(printWriter, "Regular Topic Model", "topicModel", topicModel.toString)
    ParamsWriter.appendAsXML(printWriter, "Word2Vec Model", "useWord2Vec", useWord2Vec.toString)
    ParamsWriter.appendAsXML(printWriter, "Word2Vec Merging Type", "word2VecType", word2VecType)

    printWriter.append("</PARAMETERS>\n")

  }

  def assign(paramMap: Map[String, String]): Unit = {

    lengthFeatureRates = paramMap.getOrElse("lengthFeatureRates", lengthFeatureRates.toString).toBoolean
    bowTerm = paramMap.getOrElse("bowTerm", bowTerm.toString).toBoolean
    tokenShape = paramMap.getOrElse("tokenShape", tokenShape.toString).toBoolean
    tokenEmoticon = paramMap.getOrElse("tokenEmoticon", tokenEmoticon.toString).toBoolean
    tokenSuffix = paramMap.getOrElse("tokenSuffix", tokenSuffix.toString).toBoolean

    tokenSym = paramMap.getOrElse("tokenSym", tokenSym.toString).toBoolean
    tokenGram = paramMap.getOrElse("tokenGram", tokenGram.toString).toBoolean
    tokenEmbeddings = paramMap.getOrElse("tokenEmbeddings", tokenEmbeddings.toString).toBoolean
    tokenStem = paramMap.getOrElse("tokenStem", tokenStem.toString).toBoolean
    repeatedPuncs = paramMap.getOrElse("repeatedPuncs", repeatedPuncs.toString).toBoolean
    morphStem = paramMap.getOrElse("morphStem", morphStem.toString).toBoolean
    morphTag = paramMap.getOrElse("morphTag", morphTag.toString).toBoolean
    posTag = paramMap.getOrElse("posTag", posTag.toString).toBoolean
    posTagGram = paramMap.getOrElse("posTagGram", posTagGram.toString).toBoolean
    phraseType = paramMap.getOrElse("phraseType", phraseType.toString).toBoolean
    functionWordPosTag = paramMap.getOrElse("functionWordPosTag", functionWordPosTag.toString).toBoolean
    functionWordRates = paramMap.getOrElse("functionWordRates", functionWordRates.toString).toBoolean
    functionWordHistogramRates = paramMap.getOrElse("functionWordHistogramRates", functionWordHistogramRates.toString).toBoolean
    vocabularyRichnessTokens = paramMap.getOrElse("vocabularyRichnessTokens", vocabularyRichnessTokens.toString).toBoolean
    spellSuggestedFeatures = paramMap.getOrElse("spellSuggestedFeatures", spellSuggestedFeatures.toString).toBoolean
    textShape = paramMap.getOrElse("textShape", textShape.toString).toBoolean
    textCharNGrams = paramMap.getOrElse("textCharNGrams", textCharNGrams.toString).toBoolean

    lmRegularModel = paramMap.getOrElse("lmRegularModel", lmRegularModel.toString).toBoolean
    lmTransModel = paramMap.getOrElse("lmTransModel", lmTransModel.toString).toBoolean

    topicModel = paramMap.getOrElse("topicModel", topicModel.toString).toBoolean
    useWord2Vec = paramMap.getOrElse("useWord2Vec", useWord2Vec.toString).toBoolean
    word2VecType = paramMap.getOrElse("word2VecType", word2VecType.toString).toString

  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set(
      "lengthFeatureRates", "bowTerm", "tokenShape", "tokenEmoticon", "tokenSuffix", "tokenSym", "tokenGram", "tokenEmbeddings",
      "tokenStem", "repeatedPuncs", "morphStem", "morphTag", "posTag", "posTagGram", "phraseType",
      "functionWordPosTag", "functionWordRates", "functionWordHistogramRates", "vocabularyRichnessTokens",
      "spellSuggestedFeatures", "textShape", "textCharNGrams", "lmRegularModel", "lmTransModel", "topicModel", "slicedTopicModel","useWord2Vec"
    ))
  }

  override def hashCode(): Int = {
    var result = 1
    result = result * 7 + (if (lengthFeatureRates) 1 else 0)
    result = result * 7 + (if (bowTerm) 1 else 0)
    result = result * 7 + (if (tokenShape) 1 else 0)
    result = result * 7 + (if (tokenEmoticon) 1 else 0)
    result = result * 7 + (if (tokenSuffix) 1 else 0)

    result = result * 7 + (if (tokenSym) 1 else 0)

    result = result * 7 + (if (tokenGram) 1 else 0)
    result = result * 7 + (if (tokenEmbeddings) 1 else 0)
    result = result * 7 + (if (tokenStem) 1 else 0)
    result = result * 7 + (if (repeatedPuncs) 1 else 0)
    result = result * 7 + (if (morphStem) 1 else 0)
    result = result * 7 + (if (morphTag) 1 else 0)
    result = result * 7 + (if (posTag) 1 else 0)
    result = result * 7 + (if (posTagGram) 1 else 0)
    result = result * 7 + (if (phraseType) 1 else 0)
    result = result * 7 + (if (functionWordPosTag) 1 else 0)
    result = result * 7 + (if (functionWordRates) 1 else 0)
    result = result * 7 + (if (functionWordHistogramRates) 1 else 0)
    result = result * 7 + (if (vocabularyRichnessTokens) 1 else 0)
    result = result * 7 + (if (spellSuggestedFeatures) 1 else 0)
    result = result * 7 + (if (textShape) 1 else 0)
    result = result * 7 + (if (textCharNGrams) 1 else 0)

    result = result * 7 + (if (lmTransModel) 1 else 0)
    result = result * 7 + (if (lmRegularModel) 1 else 0)

    result = result * 7 + (if (topicModel) 1 else 0)
    result = result * 7 + (if (useWord2Vec) 1 else 0)
    result = result * 7 + (if (useWord2Vec) word2VecType.hashCode else 0)

    result
  }
}

object ParamsSample {

  val name = "Dataset sampling"

  var sampleByGenre = false
  var sampleByDocType = false
  var sampleByAuthor = false
  var sampleByRate = false
  var sampleGenreCategory = "none"
  var sampleDocTypeCategory = "none"
  var sampleAuthorCategory = "none"
  var sampleRate = 1.0

  var sampleAll = false
  var sampleMinMedium = false
  var sampleMinDocSize = false

  var sampleMedium = false
  var sampleHard = false
  var sampleSoft = false
  var samplePANLarge = false
  var samplePANSmall = false


  def append(printWriter: PrintWriter): Unit = {

    printWriter.append("====" + name + "====\n")

    if (sampleAll) printWriter.append("Sample All: " + sampleAll + "\n")
    if (samplePANLarge) printWriter.append("Sample PAN Large: " + samplePANLarge + "\n")
    if (samplePANSmall) printWriter.append("Sample PAN Small: " + samplePANSmall + "\n")
    if (sampleByGenre) printWriter.append("Sample By Genre: " + sampleByGenre + "\n")
    if (sampleByDocType) printWriter.append("Sample By Genre: " + sampleByDocType + "\n")
    if (sampleByAuthor) printWriter.append("Sample By Author: " + sampleAll + "\n")
    if (sampleByRate) printWriter.append("Sample By Rate: " + sampleByRate + "\n")
    if (sampleMinMedium) printWriter.append("Sample By Satisfying Minimum Mediums: " + sampleMinMedium + "\n")
    if (sampleMinDocSize) printWriter.append("Sample By Satisfying Minimum Author-Doc Size: " + sampleMinDocSize + "\n")
    if (sampleMedium) printWriter.append("Sample Medium: " + sampleMedium + "\n")
    if (sampleHard) printWriter.append("Sample Hard: " + sampleHard + "\n")
    if (sampleSoft) printWriter.append("Sample Soft: " + sampleSoft + "\n")

    printWriter.append("Sample Genre Category: " + sampleGenreCategory + "\n")
    printWriter.append("Sample Document Type Category: " + sampleDocTypeCategory + "\n")
    printWriter.append("Sample Author Category: " + sampleAuthorCategory + "\n")
    printWriter.append("Sample Rate: " + sampleRate + "\n")

  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "All the samples", "sampleAll", sampleAll.toString)
    ParamsWriter.appendAsXML(printWriter, "Samples from PAN Large", "samplePANLarge", samplePANLarge.toString)
    ParamsWriter.appendAsXML(printWriter, "Samples from PAN Small", "samplePANSmall", samplePANSmall.toString)

    ParamsWriter.appendAsXML(printWriter, "Sampling by satisfying minimums", "sampleMinMedium", sampleMinMedium.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling by satisfying minimum author-doc size", "sampleMinDocSize", sampleMinDocSize.toString)

    ParamsWriter.appendAsXML(printWriter, "Sampling medium dataset category", "sampleMedium", sampleMedium.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling hard dataset category", "sampleHard", sampleHard.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling soft dataset category", "sampleSoft", sampleSoft.toString)

    ParamsWriter.appendAsXML(printWriter, "Sampling by certain genre types", "sampleByGenre", sampleByGenre.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling by certain document types (blog, tweet, article)", "sampleByDocType", sampleByDocType.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling by certain author names", "sampleByAuthor", sampleByAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Sampling by rate", "sampleByRate", sampleByRate.toString)

    ParamsWriter.appendAsXML(printWriter, "Sampling genres", "sampleGenreCategory", sampleGenreCategory)
    ParamsWriter.appendAsXML(printWriter, "Sampling document types (article, tweet,or blog)", "sampleDocTypeCategory", sampleDocTypeCategory)

    ParamsWriter.appendAsXML(printWriter, "Sampling authors", "sampleAuthorCategory", sampleAuthorCategory)
    ParamsWriter.appendAsXML(printWriter, "Sampling rate", "sampleRate", sampleRate.toString)
    printWriter.append("</PARAMETERS>\n")

  }

  def assign(paramMap: Map[String, String]): Unit = {
    sampleAll = paramMap.getOrElse("sampleAll", sampleAll.toString).toBoolean
    samplePANLarge = paramMap.getOrElse("samplePANLarge", samplePANLarge.toString).toBoolean
    samplePANSmall = paramMap.getOrElse("samplePANSmall", samplePANSmall.toString).toBoolean

    sampleByGenre = paramMap.getOrElse("sampleByGenre", sampleByGenre.toString).toBoolean
    sampleByDocType = paramMap.getOrElse("sampleByDocType", sampleByDocType.toString).toBoolean

    sampleByAuthor = paramMap.getOrElse("sampleByAuthor", sampleByAuthor.toString).toBoolean
    sampleByRate = paramMap.getOrElse("sampleByRate", sampleByRate.toString).toBoolean
    sampleMinMedium = paramMap.getOrElse("sampleMinMedium", sampleMinMedium.toString).toBoolean
    sampleMinDocSize = paramMap.getOrElse("sampleMinDocSize", sampleMinDocSize.toString).toBoolean

    sampleMedium = paramMap.getOrElse("sampleMedium", sampleMedium.toString).toBoolean
    sampleHard = paramMap.getOrElse("sampleHard", sampleHard.toString).toBoolean
    sampleSoft = paramMap.getOrElse("sampleSoft", sampleSoft.toString).toBoolean
    sampleGenreCategory = paramMap.getOrElse("sampleGenreCategory", sampleGenreCategory)
    sampleDocTypeCategory = paramMap.getOrElse("sampleDocTypeCategory", sampleDocTypeCategory)
    sampleAuthorCategory = paramMap.getOrElse("sampleAuthorCategory", sampleAuthorCategory)
    sampleRate = paramMap.getOrElse("sampleRate", sampleRate.toString).toDouble
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set(
      "sampleAll", "samplePANLarge", "samplePANSmall", "sampleByGenre", "sampleByDocType", "sampleByAuthor",
      "sampleByRate", "sampleMinMedium", "sampleMinDocSize", "sampleMedium", "sampleHard", "sampleSoft", "sampleGenreCategory", "sampleAuthorCategory", "sampleDocTypeCategory",
      "sampleRate"
    ))
  }

  def checkConsistency(): Unit = {
    val arr = Array(sampleAll, samplePANLarge, samplePANSmall, sampleMinMedium, sampleMinDocSize, sampleMedium, sampleHard, sampleSoft)
    val cnt = arr.count(p => {
      p
    })

    if (cnt == 0) throw new IllegalArgumentException("Inconsistency in sample parameters, only one must be true but got zero")
    else if (cnt > 1) throw new IllegalArgumentException("Inconsistency in sample parameters, only one must be true but got more than one")

  }

  override def hashCode(): Int = {

    var result = 7

    result = 31 * result + (if (sampleAll) 1 else 0)
    result = 31 * result + (if (samplePANLarge) 1 else 0)
    result = 31 * result + (if (samplePANSmall) 1 else 0)

    result = 31 * result + (if (sampleByGenre) 1 else 0)
    result = 31 * result + (if (sampleByDocType) 1 else 0)

    result = 31 * result + (if (sampleByAuthor) 1 else 0)
    result = 31 * result + (if (sampleByRate) 1 else 0)
    result = 31 * result + (if (sampleMinMedium) 1 else 0)
    result = 31 * result + (if (sampleMinDocSize) 1 else 0)
    result = 31 * result + (if (sampleMedium) 1 else 0)
    result = 31 * result + (if (sampleHard) 1 else 0)
    result = 31 * result + (if (sampleSoft) 1 else 0)

    if (sampleByGenre) {
      result = 31 * result + sampleGenreCategory.hashCode
    }

    if (sampleByDocType) {
      result = 31 * result + sampleDocTypeCategory.hashCode
    }

    if (sampleByAuthor) {
      result = 31 * result + sampleAuthorCategory.hashCode
    }

    if (sampleByRate) {
      result = 31 * result + sampleRate.hashCode()
    }
    result
  }


}

object ParamsClassification {

  val name = "Classification Parameters"

  var createWekaDataset = false
  var createLibSVMDataset = false
  var createTypeLMText = false
  var createHLDAmodel = false
  var createLDAmodel = false
  var createWord2Vec = false

  var classBasedAssemble = false
  var naiveBayesAuthor = false
  var logisticAuthor = false
  var svmAuthor = false
  var svmRBFAuthor = false
  var svmDotAuthor = false
  var svmTransductiveAuthor = false
  var dtTreeAuthor = false
  var rndForestAuthor = false
  var mpAuthor = false

  var logisticMaxIter = 500
  var logisticTol = 1E-4
  var svmMaxIter = 3000
  var svmPPackSize = 2
  var svmMiniBatchFraction = 0.4
  var svmRegParameter = 1.0
  var svmTol = 1E-4
  var rbfGamma = 100.0

  var maxDepth = 15
  var maxBins = 64
  var rndNumTrees = 10

  var layerSize = Array(30, 15)
  var mpTol = 1E-5
  var mpMaxIter = 100

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====" + name + "====\n")

    if (createWekaDataset) printWriter.append("Create Weka (ARFF) Dataset: " + createWekaDataset + "\n")
    if (createLibSVMDataset) printWriter.append("Create LibSVM Dataset: " + createLibSVMDataset + "\n")
    if (createTypeLMText) printWriter.append("Create Language Models by Genre/DocType : " + createTypeLMText + "\n")
    if (createHLDAmodel) printWriter.append("Create Hierarchical Topic Models : " + createHLDAmodel + "\n")
    if (createLDAmodel) printWriter.append("Create Topic Models : " + createLDAmodel + "\n")
    if (createWord2Vec) printWriter.append("Create Word2Vec Model : " + createWord2Vec + "\n")

    if (classBasedAssemble) printWriter.append("Assemble Class-Based Classification: " + classBasedAssemble + "\n")
    if (naiveBayesAuthor) printWriter.append("Naive Bayes Classification: " + naiveBayesAuthor + "\n")
    if (logisticAuthor) printWriter.append("Logistic Regression Classification: " + logisticAuthor + "\n")
    if (svmAuthor) printWriter.append("Linear SVM Classification: " + svmAuthor + "\n")
    if (svmRBFAuthor) printWriter.append("RBF Kernel SVM (Dual) Classification: " + svmRBFAuthor + "\n")
    if (svmDotAuthor) printWriter.append("DOT Product Kernel SVM (Dual) Classification: " + svmDotAuthor + "\n")
    if (svmTransductiveAuthor) printWriter.append("DOT Product Kernel SVM by SMO Training: " + svmTransductiveAuthor + "\n")
    if (dtTreeAuthor) printWriter.append("Decision Tree Classifier: " + dtTreeAuthor + "\n")
    if (rndForestAuthor) printWriter.append("Random Forest Classifier: " + rndForestAuthor + "\n")

    if (mpAuthor) printWriter.append("Multilayer Perceptron Classifier: " + mpAuthor + "\n")


    printWriter.append("Logistic Regression Max Iteration: " + logisticMaxIter + "\n")
    printWriter.append("Logistic Tol: " + logisticTol + "\n")
    printWriter.append("Linear SVM Max Iteration: " + svmMaxIter + "\n")
    printWriter.append("SVM PPack Size (r): " + svmPPackSize + "\n")
    printWriter.append("SVM Mini Batch Fraction: " + svmMiniBatchFraction + "\n")
    printWriter.append("SVM Tol: " + svmTol + "\n")
    printWriter.append("SVM Regression Parameter: " + svmRegParameter + "\n")

    printWriter.append("RBF Gamma: " + rbfGamma + "\n")
    printWriter.append("Max Decision Tree Depth: " + maxDepth + "\n")
    printWriter.append("Max Decision Tree Bins: " + maxBins + "\n")
    printWriter.append("Maximum Number of Trees: " + rndNumTrees + "\n")

    printWriter.append("Multilayer Perceptron Layer Size: " + layerSize.mkString("|") + "\n")
    printWriter.append("Multilayer Perceptron ToL: " + mpTol + "\n")
    printWriter.append("Multilayer Perceptron Maximum Iteration: " + mpMaxIter + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")

    ParamsWriter.appendAsXML(printWriter, "Create a WEKA (ARFF) Dataset", "createWekaDataset", createWekaDataset.toString)
    ParamsWriter.appendAsXML(printWriter, "Create a LibSVM Dataset", "createLibSVMDataset", createLibSVMDataset.toString)
    ParamsWriter.appendAsXML(printWriter, "Create type language models ", "createTypeLMText", createTypeLMText.toString)
    ParamsWriter.appendAsXML(printWriter, "Create hierarchical topic models ", "createHLDAmodel", createHLDAmodel.toString)
    ParamsWriter.appendAsXML(printWriter, "Create topic models ", "createLDAmodel", createLDAmodel.toString)
    ParamsWriter.appendAsXML(printWriter, "Create word2vec model", "createWord2Vec", createWord2Vec.toString)

    ParamsWriter.appendAsXML(printWriter, "Author classification by class-based weighting", "classBasedAssemble", classBasedAssemble.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by naive bayes", "naiveBayesAuthor", naiveBayesAuthor.toString)

    ParamsWriter.appendAsXML(printWriter, "Author classification by logistic regression", "logisticAuthor", logisticAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by linear svm", "svmAuthor", svmAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by dual RBF svm", "svmRBFAuthor", svmRBFAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by dual Dot product svm", "svmDotAuthor", svmDotAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by SMO Training", "svmSMOAuthor", svmTransductiveAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by Decision Tree Classifier", "dtTreeAuthor", dtTreeAuthor.toString)
    ParamsWriter.appendAsXML(printWriter, "Author classification by Random Tree Classifier", "rndForestAuthor", rndForestAuthor.toString)

    ParamsWriter.appendAsXML(printWriter, "Author classification by Multilayer Perceptron Classifier", "mpAuthor", mpAuthor.toString)

    ParamsWriter.appendAsXML(printWriter, "Logistic Regression Max Iteration", "logisticMaxIter", logisticMaxIter.toString)
    ParamsWriter.appendAsXML(printWriter, "Logistic TOL", "logisticTol", logisticTol.toString)
    ParamsWriter.appendAsXML(printWriter, "Linear SVM Max Iteration", "svmMaxIter", svmMaxIter.toString)
    ParamsWriter.appendAsXML(printWriter, "SVM PPack Size (r)", "svmPPackSize", svmPPackSize.toString)
    ParamsWriter.appendAsXML(printWriter, "SVM Mini Batch Fraction", "svmMiniBatchFraction", svmMiniBatchFraction.toString)
    ParamsWriter.appendAsXML(printWriter, "SVM Regression Parameter", "svmRegParameter", svmRegParameter.toString)

    ParamsWriter.appendAsXML(printWriter, "SVM TOL", "svmTol", svmTol.toString)
    ParamsWriter.appendAsXML(printWriter, "RBF Gamma Parameter", "rbfGamma", rbfGamma.toString)

    ParamsWriter.appendAsXML(printWriter, "Maximum DTree Depth", "maxDepth", maxDepth.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum DTree Bins", "maxBins", maxBins.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Number of Random Forest Trees", "rndNumTrees", rndNumTrees.toString)

    ParamsWriter.appendAsXML(printWriter, "Multilayer Perceptron Layer Size", "layerSize", layerSize.map(value => value.toString).mkString("|"))
    ParamsWriter.appendAsXML(printWriter, "Multilayer Perceptron Tol", "mpTol", mpTol.toString)
    ParamsWriter.appendAsXML(printWriter, "Multilayer Perceptron Maximum Iteration", "mpMaxIter", mpMaxIter.toString)


    printWriter.append("</PARAMETERS>\n")

  }

  def assign(paramMap: Map[String, String]): Unit = {

    createWekaDataset = paramMap.getOrElse("createWekaDataset", createWekaDataset.toString).toBoolean
    createLibSVMDataset = paramMap.getOrElse("createLibSVMDataset", createLibSVMDataset.toString).toBoolean
    createTypeLMText = paramMap.getOrElse("createTypeLMText", createTypeLMText.toString).toBoolean
    createHLDAmodel = paramMap.getOrElse("createHLDAmodel", createHLDAmodel.toString).toBoolean
    createLDAmodel = paramMap.getOrElse("createLDAmodel", createLDAmodel.toString).toBoolean
    createWord2Vec = paramMap.getOrElse("createWord2Vec", createWord2Vec.toString).toBoolean

    classBasedAssemble = paramMap.getOrElse("classBasedAssemble", classBasedAssemble.toString).toBoolean
    naiveBayesAuthor = paramMap.getOrElse("naiveBayesAuthor", naiveBayesAuthor.toString).toBoolean

    logisticAuthor = paramMap.getOrElse("logisticAuthor", logisticAuthor.toString).toBoolean
    svmAuthor = paramMap.getOrElse("svmAuthor", svmAuthor.toString).toBoolean
    svmRBFAuthor = paramMap.getOrElse("svmRBFAuthor", svmRBFAuthor.toString).toBoolean
    svmDotAuthor = paramMap.getOrElse("svmDotAuthor", svmDotAuthor.toString).toBoolean
    svmTransductiveAuthor = paramMap.getOrElse("svmSMOAuthor", svmTransductiveAuthor.toString).toBoolean
    dtTreeAuthor = paramMap.getOrElse("dtTreeAuthor", dtTreeAuthor.toString).toBoolean
    rndForestAuthor = paramMap.getOrElse("rndForestAuthor", rndForestAuthor.toString).toBoolean

    mpAuthor = paramMap.getOrElse("mpAuthor", mpAuthor.toString).toBoolean

    logisticMaxIter = paramMap.getOrElse("logisticMaxIter", logisticMaxIter.toString).toInt
    logisticTol = paramMap.getOrElse("logisticTol", logisticTol.toString).toDouble
    svmMaxIter = paramMap.getOrElse("svmMaxIter", svmMaxIter.toString).toInt
    svmPPackSize = paramMap.getOrElse("svmPPackSize", svmPPackSize.toString).toInt
    svmMiniBatchFraction = paramMap.getOrElse("svmMiniBatchFraction", svmMiniBatchFraction.toString).toDouble
    svmRegParameter = paramMap.getOrElse("svmRegParameter", svmRegParameter.toString).toDouble

    svmTol = paramMap.getOrElse("svmTol", svmTol.toString).toDouble
    rbfGamma = paramMap.getOrElse("rbfGamma", rbfGamma.toString).toDouble

    maxDepth = paramMap.getOrElse("maxDepth", maxDepth.toString).toInt
    maxBins = paramMap.getOrElse("maxBins", maxBins.toString).toInt
    rndNumTrees = paramMap.getOrElse("rndNumTrees", rndNumTrees.toString).toInt

    mpTol = paramMap.getOrElse("mpTol", mpTol.toString).toDouble
    mpMaxIter = paramMap.getOrElse("mpMaxIter", mpMaxIter.toString).toInt
    layerSize = paramMap.getOrElse("layerSize", layerSize.map(d => d.toString).mkString("|"))
      .split("\\|").map(value => value.toInt)

  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set(
      "createWekaDataset", "createLibSVMDataset", "createTypeLMText", "createTopicModel","createWord2Vec", "classBasedAssemble", "naiveBayesAuthor",
      "logisticAuthor", "svmAuthor",
      "svmRBFAuthor", "svmDotAuthor", "svmSMOAuthor", "dtTreeAuthor", "rndForestAuthor",
      "mpAuthor", "logisticMaxIter", "logisticTol",
      "svmMaxIter", "svmPPackSize", "svmMiniBatchFraction", "svmRegParameter", "svmTol", "rbfGamma", "maxDepth",
      "maxBins","rndNumTrees", "layerSize", "mpTol", "mpMaxIter"
    ))
  }

  def checkConsistency(): Unit = {
    val arr = Array(createWekaDataset, createLibSVMDataset, createTypeLMText, createHLDAmodel,createLDAmodel,createWord2Vec, naiveBayesAuthor, logisticAuthor, svmAuthor, svmDotAuthor, svmRBFAuthor,
      svmTransductiveAuthor, dtTreeAuthor,rndForestAuthor, mpAuthor)
    val cnt = arr.count(p => {
      p
    })

    if (cnt == 0) throw new IllegalArgumentException("Inconsistency in classification parameters, only one must be true but got zero")
    else if (cnt > 1) throw new IllegalArgumentException("Inconsistency in classification parameters, only one must be true but got more than one")
  }

  override def hashCode(): Int = {

    var result = 1

    result = 7 * result + (if (createWekaDataset) 1 else 0)
    result = 7 * result + (if (createLibSVMDataset) 1 else 0)
    result = 7 * result + (if (createTypeLMText) 1 else 0)
    result = 7 * result + (if (createHLDAmodel) 1 else 0)
    result = 7 * result + (if (createLDAmodel) 1 else 0)
    result = 7 * result + (if (createWord2Vec) 1 else 0)

    result = 7 * result + (if (classBasedAssemble) 1 else 0)
    result = 7 * result + (if (naiveBayesAuthor) 1 else 0)
    result = 7 * result + (if (logisticAuthor) 1 else 0)
    result = 7 * result + (if (svmAuthor) 1 else 0)
    result = 7 * result + (if (svmRBFAuthor) 1 else 0)
    result = 7 * result + (if (svmDotAuthor) 1 else 0)
    result = 7 * result + (if (svmTransductiveAuthor) 1 else 0)
    result = 7 * result + (if (dtTreeAuthor) 1 else 0)
    result = 7 * result + (if (rndForestAuthor) 1 else 0)
    result = 7 * result + (if (mpAuthor) 1 else 0)

    if (logisticAuthor) {
      result = 7 * result + logisticMaxIter
      result = 7 * result + logisticTol.hashCode()
    }

    if (svmAuthor || svmRBFAuthor || svmDotAuthor || svmTransductiveAuthor) {
      result = 7 * result + svmMaxIter
      result = 7 * result + svmMiniBatchFraction.hashCode()
      result = 7 * result + svmRegParameter.hashCode()
      result = 7 * result + svmTol.hashCode()
    }

    if (svmRBFAuthor || svmDotAuthor) {
      result = 7 * result + svmPPackSize
    }

    if (svmRBFAuthor) {
      result = 7 * result + rbfGamma.hashCode()
    }

    if (dtTreeAuthor) {
      result = 7 * result + maxDepth
      result = 7 * result + maxBins
    }

    if(rndForestAuthor){
      result = 7 * result + rndNumTrees
    }

    if (mpAuthor) {
      result = 7 * result + layerSize.hashCode()
      result = 7 * result + mpTol.hashCode()
      result = 7 * result + mpMaxIter.hashCode()
    }

    result

  }

}

object ParamsEvaluation {

  val name = "Evaluation Parameters"

  var trainTestSplit = Array(0.9, 0.1)
  var trainMinSize = 100
  var testMinSize = 20
  var stratified = false
  var random = false
  var crossGenre = false
  var kfold = 10


  var printConfusion = true
  var measureCovariate = false
  var covariateByClassification = false
  var covariateByKLDivergence = false
  var covariateByCMD = false
  var srcDstGenre = Array[String]("politics","twitter")

  def toMap(): Map[String, Set[String]] = {
    Map(name -> Set("trainTestSplit", "trainMinSize","stratified","random","kfold", "measureCovariate"))
  }

  def checkConsistency(): Unit = {
    val arr = Array(covariateByClassification, covariateByKLDivergence, covariateByCMD)
    val cnt = arr.count(p => {
      p
    })

    if (cnt > 1) throw new IllegalArgumentException("Inconsistency in evaluation parameters, only one must be true but got more than one")
  }

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("====Evaluation Results====\n")
    printWriter.append("PrintConfusion: " + printConfusion + "\n")
    printWriter.append("MeasureCovariate: " + measureCovariate + "\n")
    printWriter.append("covariateByClassification : " + covariateByClassification + "\n")
    printWriter.append("covariateByKLDivergence: " + covariateByKLDivergence + "\n")
    printWriter.append("covariateByCMD: " + covariateByCMD + "\n")

    printWriter.append("Cross Genre Tests: " + crossGenre + "\n")
    printWriter.append("Source and destination cross genres: " + srcDstGenre.map(d => d.toString).mkString("|") + "\n")
    printWriter.append("Train/Test Split: " + trainTestSplit.map(d => d.toString).mkString("|") + "\n")
    printWriter.append("Train Min Size: " + trainMinSize.toString + "\n")
    printWriter.append("Test Min Size: " + testMinSize.toString + "\n")
    printWriter.append("Stratified k-fold cross validation: " + stratified.toString + "\n")
    printWriter.append("Stratified random k-fold cross validation: " + random.toString + "\n")
    printWriter.append("K-fold : " + kfold.toString + "\n")

  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Do print confusion matrix", "printConfusion", printConfusion.toString)
    ParamsWriter.appendAsXML(printWriter, "Do measure covariate", "measureCovariate", measureCovariate.toString)
    ParamsWriter.appendAsXML(printWriter, "Do measure covariate by classification", "covariateByClassification", covariateByClassification.toString)
    ParamsWriter.appendAsXML(printWriter, "Do measure covariate by kl-divergence", "covariateByKLDivergence", covariateByKLDivergence.toString)
    ParamsWriter.appendAsXML(printWriter, "Do measure covariate by correlation matrix distance", "covariateByCMD", covariateByCMD.toString)
    ParamsWriter.appendAsXML(printWriter, "Cross Genre Tests", "crossGenre", crossGenre.toString)
    ParamsWriter.appendAsXML(printWriter, "Cross Genre Source and Destination", "srcDstGenre", srcDstGenre.map(d => d.toString).mkString("|"))
    ParamsWriter.appendAsXML(printWriter, "Stratified k-fold cross validation", "stratified", stratified.toString)
    ParamsWriter.appendAsXML(printWriter, "Stratified random k-fold cross validation", "random", random.toString)
    ParamsWriter.appendAsXML(printWriter, "K-fold", "kfold", kfold.toString)

    ParamsWriter.appendAsXML(printWriter, "Minimum training instance size", "trainMinSize", trainMinSize.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum testing instance size", "testMinSize", testMinSize.toString)
    ParamsWriter.appendAsXML(printWriter, "Train/Test Percentage Split", "trainTestSplit", trainTestSplit.map(d => d.toString).mkString("|"))

    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    printConfusion = paramMap.getOrElse("printConfusion", printConfusion.toString).toBoolean
    measureCovariate = paramMap.getOrElse("measureCovariate", measureCovariate.toString).toBoolean
    covariateByClassification = paramMap.getOrElse("covariateByClassification", covariateByClassification.toString).toBoolean
    covariateByKLDivergence = paramMap.getOrElse("covariateByKLDivergence", covariateByKLDivergence.toString).toBoolean
    stratified = paramMap.getOrElse("stratified", stratified.toString).toBoolean
    random = paramMap.getOrElse("random", random.toString).toBoolean

    covariateByCMD = paramMap.getOrElse("covariateByCMD", covariateByCMD.toString).toBoolean
    crossGenre = paramMap.getOrElse("crossGenre", crossGenre.toString).toBoolean

    trainMinSize = paramMap.getOrElse("trainMinSize", trainMinSize.toString).toInt
    testMinSize = paramMap.getOrElse("testMinSize", testMinSize.toString).toInt
    kfold = paramMap.getOrElse("kfold", kfold.toString).toInt

    trainTestSplit = paramMap.getOrElse("trainTestSplit", trainTestSplit
      .map(d => d.toString).mkString("|"))
      .split("\\|").map(value => value.toDouble)

    srcDstGenre = paramMap.getOrElse("srcDstGenre", srcDstGenre
      .map(d => d.toString).mkString("|"))
      .split("\\|").map(value => value.toString)
  }

  override def hashCode(): Int = {
    var result = 1
    result = 7 * result + trainMinSize
    result = 7 * result + testMinSize
    result = 7 * result + trainTestSplit(0).hashCode()
    result = 7 * result + trainTestSplit(1).hashCode()

    result = 7 * result + srcDstGenre(0).hashCode()
    result = 7 * result + srcDstGenre(1).hashCode()

    result = 7 * result + (if(stratified) 1 else 0)
    result = 7 * result + (if(random) 1 else 0)
    result = 7 * result + (if(crossGenre) 1 else 0)
    result = 7 * result + kfold.hashCode()

    result
  }
}

object ParamsWeighting {

  val name = "Weighting Parameters"


  var sentenceShapeFeatureWeighting = true
  var stemFeatureWeighting = true
  var termFeatureWeighting = true
  var textShapeFeatureWeighting = true
  var tokenGramFeatureWeighting = true
  var posGramFeatureWeighting = true
  var repeatedPuncFeatureWeighting = true
  var textCharGramFeatureWeighting = true
  var phraselTypeFeatureWeighting = true
  var functionPOSFeatureWeighting = true
  var tokenShapeFeatureWeighting = true
  var tokenSuffixFeatureWeighting = true
  var tokenEmoticonFeatureWeighting = true
  var tokenSymbolFeatureWeighting = true
  var posTagFeatureWeighting = true
  var morphTagFeatureWeighting = true


  var tfidf = true
  var chiSquare = false
  var corrCoefficient = false
  var infoGain = false
  var oddsRatio = false
  var probLiu = false
  var normalization = true
  var normLevel = 2d

  var maxTF = 1000000
  var midHighTF = 100000
  var midLowTF = 10000
  var minTF = 100


  var minDocFrequency = 1
  var softDocFrequency = 2
  var midLessDocFrequency = 5
  var midMoreDocFrequency = 10
  var midHighDocFrequency = 14
  var maxDocFrequency = 20


  def append(printWriter: PrintWriter): Unit = {

    printWriter.append("====" + name + "====\n")

    if (termFeatureWeighting) printWriter.append("Term Feature Weighting: " + termFeatureWeighting + "\n")
    if (stemFeatureWeighting) printWriter.append("Stem Feature Weighting: " + stemFeatureWeighting + "\n")
    if (sentenceShapeFeatureWeighting) printWriter.append("Sentence Shape  Feature Weighting: " + sentenceShapeFeatureWeighting + "\n")
    if (textShapeFeatureWeighting) printWriter.append("Text Shape Feature Weighting: " + textShapeFeatureWeighting + "\n")
    if (tokenGramFeatureWeighting) printWriter.append("Token Gram Feature Weighting: " + tokenGramFeatureWeighting + "\n")
    if (repeatedPuncFeatureWeighting) printWriter.append("Repeated Punctuations Feature Weighting: " + repeatedPuncFeatureWeighting + "\n")
    if (textCharGramFeatureWeighting) printWriter.append("Text Char NGram Feature Weighting: " + textCharGramFeatureWeighting + "\n")
    if (phraselTypeFeatureWeighting) printWriter.append("Phrasel Type Feature Weighting: " + phraselTypeFeatureWeighting + "\n")
    if (functionPOSFeatureWeighting) printWriter.append("Function Word POS Tag Feature Weighting: " + functionPOSFeatureWeighting + "\n")
    if (tokenShapeFeatureWeighting) printWriter.append("Token Shape Feature Weighting: " + tokenShapeFeatureWeighting + "\n")
    if (tokenSymbolFeatureWeighting) printWriter.append("Token Symbol Feature Weighting: " + tokenSymbolFeatureWeighting + "\n")
    if (tokenSuffixFeatureWeighting) printWriter.append("Token Suffix/Prefix Feature Weighting: " + tokenSuffixFeatureWeighting + "\n")
    if (tokenEmoticonFeatureWeighting) printWriter.append("Token Emoticon Feature Weighting: " + tokenEmoticonFeatureWeighting + "\n")
    if (posTagFeatureWeighting) printWriter.append("Token Emoticon Feature Weighting: " + tokenEmoticonFeatureWeighting + "\n")
    if (posGramFeatureWeighting) printWriter.append("POS Tag Gram Feature Weighting: " + posGramFeatureWeighting + "\n")
    if (morphTagFeatureWeighting) printWriter.append("Morphological Tag Feature Weighting: " + morphTagFeatureWeighting + "\n")




    if (tfidf) printWriter.append("TF-IDF: " + tfidf + "\n")
    if (chiSquare) printWriter.append("Chi-Square: " + chiSquare + "\n")
    if (corrCoefficient) printWriter.append("Correlation Coefficient: " + corrCoefficient + "\n")
    if (infoGain) printWriter.append("Info Gain: " + infoGain + "\n")
    if (oddsRatio) printWriter.append("Odds Ratio: " + oddsRatio + "\n")
    if (probLiu) printWriter.append("Prob LIU: " + probLiu + "\n")
    if (normalization) printWriter.append("Normalization: " + normalization + "\n")
    if (normalization) printWriter.append("Normalization Level (P-Norm): " + normLevel + "\n")


  }

  def appendAsXML(printWriter: PrintWriter): Unit = {

    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Term Feature Weighting", "termFeatureWeighting", termFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Stem Feature Weighting", "stemFeatureWeighting", stemFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Shape Feature Weighting", "shapeFeatureWeighting", sentenceShapeFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Text Shape Feature Weighting", "textShapeFeatureWeighting", textShapeFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Gram Feature Weighting", "tokenGramFeatureWeighting", tokenGramFeatureWeighting.toString)

    ParamsWriter.appendAsXML(printWriter, "POS Tag Gram Feature Weighting", "posGramFeatureWeighting", posGramFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "POS Tag Feature Weighting", "posTagFeatureWeighting", posTagFeatureWeighting.toString)

    ParamsWriter.appendAsXML(printWriter, "Morph Tag Feature Weighting", "morphTagFeatureWeighting", morphTagFeatureWeighting.toString)

    ParamsWriter.appendAsXML(printWriter, "Repeated Punctuation Feature Weighting", "repeatedPuncFeatureWeighting", repeatedPuncFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Text Char N-Gram Feature Weighting", "textCharGramFeatureWeighting", textCharGramFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Phrasel Type Feature Weighting", "phraselTypeFeatureWeighting", phraselTypeFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Function Word POS Tag Feature Weighting", "functionPOSFeatureWeighting", functionPOSFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Shape Feature Weighting", "tokenShapeFeatureWeighting", tokenShapeFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Symbol Feature Weighting", "tokenSymbolFeatureWeighting", tokenSymbolFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Emoticon Feature Weighting", "tokenEmoticonFeatureWeighting", tokenEmoticonFeatureWeighting.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Suffix/Prefix Feature Weighting", "tokenSuffixFeatureWeighting", tokenSuffixFeatureWeighting.toString)


    ParamsWriter.appendAsXML(printWriter, "TF/IDF Weighting", "tfidf", tfidf.toString)
    ParamsWriter.appendAsXML(printWriter, "ChiSquare Weighting", "chiSquare", chiSquare.toString)
    ParamsWriter.appendAsXML(printWriter, "Correlation Coefficient Weighting", "corrCoefficient", corrCoefficient.toString)
    ParamsWriter.appendAsXML(printWriter, "Information Gain Weighting", "infoGain", infoGain.toString)
    ParamsWriter.appendAsXML(printWriter, "Odds Ratio Weighting", "oddsRatio", oddsRatio.toString)
    ParamsWriter.appendAsXML(printWriter, "ProbLiu Weighting", "probLiu", probLiu.toString)
    ParamsWriter.appendAsXML(printWriter, "Normalizing Feature Vector (P-Norm)", "normalization", normalization.toString)
    ParamsWriter.appendAsXML(printWriter, "Normalizing Level (P-Number)", "normLevel", normLevel.toString)

    ParamsWriter.appendAsXML(printWriter, "Maximum Number of TF Feature (Maximum)", "maxTF", maxTF.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Number of TF Feature (Higher)", "midHighTF", midHighTF.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Number of TF Feature (Lower)", "midLowTF", midLowTF.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Number of TF Feature (Minimum)", "minTF", minTF.toString)

    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (1)", "minDocFrequency", minDocFrequency.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (2 or more)", "softDocFrequency", softDocFrequency.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (4 or more)", "midLessDocFrequency", midLessDocFrequency.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (6 or more)", "midMoreDocFrequency", midMoreDocFrequency.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (10 or more)", "midHighDocFrequency", midHighDocFrequency.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Feature Doc Count (20 or more)", "maxDocFrequency", maxDocFrequency.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {

    termFeatureWeighting = paramMap.getOrElse("termFeatureWeighting", termFeatureWeighting.toString).toBoolean
    stemFeatureWeighting = paramMap.getOrElse("stemFeatureWeighting", stemFeatureWeighting.toString).toBoolean
    sentenceShapeFeatureWeighting = paramMap.getOrElse("shapeFeatureWeighting", sentenceShapeFeatureWeighting.toString).toBoolean
    textShapeFeatureWeighting = paramMap.getOrElse("textShapeFeatureWeighting", textShapeFeatureWeighting.toString).toBoolean
    tokenGramFeatureWeighting = paramMap.getOrElse("tokenGramFeatureWeighting", tokenGramFeatureWeighting.toString).toBoolean
    posGramFeatureWeighting = paramMap.getOrElse("posGramFeatureWeighting", posGramFeatureWeighting.toString).toBoolean
    posTagFeatureWeighting = paramMap.getOrElse("posTagFeatureWeighting", posTagFeatureWeighting.toString).toBoolean

    morphTagFeatureWeighting = paramMap.getOrElse("morphTagFeatureWeighting", morphTagFeatureWeighting.toString).toBoolean


    repeatedPuncFeatureWeighting = paramMap.getOrElse("repeatedPuncFeatureWeighting", repeatedPuncFeatureWeighting.toString).toBoolean
    textCharGramFeatureWeighting = paramMap.getOrElse("textCharGramFeatureWeighting", textCharGramFeatureWeighting.toString).toBoolean
    phraselTypeFeatureWeighting = paramMap.getOrElse("phraselTypeFeatureWeighting", phraselTypeFeatureWeighting.toString).toBoolean
    functionPOSFeatureWeighting = paramMap.getOrElse("functionPOSFeatureWeighting", functionPOSFeatureWeighting.toString).toBoolean
    tokenShapeFeatureWeighting = paramMap.getOrElse("tokenShapeFeatureWeighting", tokenShapeFeatureWeighting.toString).toBoolean
    tokenSymbolFeatureWeighting = paramMap.getOrElse("tokenSymbolFeatureWeighting", tokenSymbolFeatureWeighting.toString).toBoolean
    tokenEmoticonFeatureWeighting = paramMap.getOrElse("tokenEmoticonFeatureWeighting", tokenEmoticonFeatureWeighting.toString).toBoolean
    tokenSuffixFeatureWeighting = paramMap.getOrElse("tokenSuffixFeatureWeighting", tokenSuffixFeatureWeighting.toString).toBoolean


    maxTF = paramMap.getOrElse("maxTF", maxTF.toString).toInt
    midHighTF = paramMap.getOrElse("midHighTF", midHighTF.toString).toInt
    midLowTF = paramMap.getOrElse("midLowTF", midLowTF.toString).toInt
    minTF = paramMap.getOrElse("minTF", minTF.toString).toInt

    minDocFrequency = paramMap.getOrElse("minDocFrequency", minDocFrequency.toString).toInt
    softDocFrequency = paramMap.getOrElse("softDocFrequency", softDocFrequency.toString).toInt
    midLessDocFrequency = paramMap.getOrElse("midLessDocFrequency", midLessDocFrequency.toString).toInt
    midMoreDocFrequency = paramMap.getOrElse("midMoreDocFrequency", midMoreDocFrequency.toString).toInt
    midHighDocFrequency = paramMap.getOrElse("midHighDocFrequency", midHighDocFrequency.toString).toInt
    maxDocFrequency = paramMap.getOrElse("maxDocFrequency", maxDocFrequency.toString).toInt


    tfidf = paramMap.getOrElse("tfidf", tfidf.toString).toBoolean
    probLiu = paramMap.getOrElse("probLiu", probLiu.toString).toBoolean
    chiSquare = paramMap.getOrElse("chiSquare", chiSquare.toString).toBoolean
    infoGain = paramMap.getOrElse("infoGain", infoGain.toString).toBoolean
    oddsRatio = paramMap.getOrElse("oddsRatio", oddsRatio.toString).toBoolean
    corrCoefficient = paramMap.getOrElse("corrCoefficient", corrCoefficient.toString).toBoolean

    normalization = paramMap.getOrElse("normalization", normalization.toString).toBoolean
    normLevel = paramMap.getOrElse("normLevel", normLevel.toString).toDouble
  }

  def toMap(): Map[String, Set[String]] = {
    Map(name -> Set("tfidf", "probLiu", "chiSquare", "infoGain", "oddsRatio", "corrCoefficient", "normalization", "normLevel"))
  }

  override def hashCode(): Int = {
    var result: Int = 1

    result = 7 * result + (if (termFeatureWeighting) 1 else 0)
    result = 7 * result + (if (stemFeatureWeighting) 1 else 0)
    result = 7 * result + (if (sentenceShapeFeatureWeighting) 1 else 0)
    result = 7 * result + (if (textShapeFeatureWeighting) 1 else 0)
    result = 7 * result + (if (tokenGramFeatureWeighting) 1 else 0)
    result = 7 * result + (if (posGramFeatureWeighting) 1 else 0)
    result = 7 * result + (if (posTagFeatureWeighting) 1 else 0)

    result = 7 * result + (if (repeatedPuncFeatureWeighting) 1 else 0)
    result = 7 * result + (if (textCharGramFeatureWeighting) 1 else 0)
    result = 7 * result + (if (phraselTypeFeatureWeighting) 1 else 0)
    result = 7 * result + (if (functionPOSFeatureWeighting) 1 else 0)
    result = 7 * result + (if (tokenShapeFeatureWeighting) 1 else 0)
    result = 7 * result + (if (normalization) 1 else 0)
    result = 7 * result + (if (normalization) normLevel.toInt else 0)


    result = 7 * result + maxTF
    result = 7 * result + midHighTF
    result = 7 * result + midLowTF
    result = 7 * result + minTF

    result = 7 * result + maxDocFrequency
    result = 7 * result + minDocFrequency
    result = 7 * result + midLessDocFrequency
    result = 7 * result + midHighDocFrequency
    result = 7 * result + midMoreDocFrequency
    result = 7 * result + softDocFrequency


    result

  }
}

object ParamsSelection {

  val name = "Feature Selection"
  var termSelection = false
  var stemSelection = false
  var tokenGramSelection = false
  var textGramSelection = false
  var tokenSuffixSelection = false

  var pcaSelection = false
  var chiSquareSelection = false
  var chiSquareBinSize = 1000
  var pcaK = 100
  var chiK = 500

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")
    if (termSelection) printWriter.append("Term (BOW) Feature Selection: " + termSelection + "\n")
    if (stemSelection) printWriter.append("Stem Feature Selection: " + stemSelection + "\n")
    if (tokenGramSelection) printWriter.append("Token Gram Feature Selection: " + tokenGramSelection + "\n")
    if (textGramSelection) printWriter.append("Text (Char) Gram Feature Selection: " + textGramSelection + "\n")
    if (tokenSuffixSelection) printWriter.append("Token Suffix Feature Selection: " + tokenSuffixSelection + "\n")
    if (pcaSelection) printWriter.append("Principal Component Analysis Selection: " + pcaSelection + "\n")
    if (pcaSelection) printWriter.append("K Number of Principal Components: " + pcaK.toString + "\n")

    //TODO : Buketizer is needed for categorical features
    if (chiSquareSelection) printWriter.append("ChiSquare Categorical Selection: " + chiSquareSelection + "\n")
    if (chiSquareSelection) printWriter.append("ChiSquare Number of Categorical Features: " + chiK.toString + "\n")
    if (chiSquareSelection) printWriter.append("ChiSquare Number of Bins" + chiSquareBinSize.toString + "\n")

  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Term (BOW) Feature Selection", "termSelection", termSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "Stem Feature Selection", "stemSelection", stemSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Gram Feature Selection", "tokenGramSelection", tokenGramSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "Text (Char) Gram Feature Selection", "textGramSelection", textGramSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "Token Suffix Gram Feature Selection", "tokenSuffixSelection", tokenSuffixSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "Principal Component Analysis Selection", "pcaSelection", pcaSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "K Number of Principal Components", "pcaK", pcaK.toString)

    ParamsWriter.appendAsXML(printWriter, "ChiSquare Categorical Selection", "chiSquareSelection", chiSquareSelection.toString)
    ParamsWriter.appendAsXML(printWriter, "ChiSquare Number of Bins", "chiSquareBinSize", chiSquareBinSize.toString)
    ParamsWriter.appendAsXML(printWriter, "ChiSquare Number of Categorical Features", "chiK", chiK.toString)


    printWriter.append("</PARAMETERS>\n")
  }

  def assign(paramMap: Map[String, String]): Unit = {
    termSelection = paramMap.getOrElse("termSelection", termSelection.toString).toBoolean
    stemSelection = paramMap.getOrElse("stemSelection", stemSelection.toString).toBoolean
    tokenGramSelection = paramMap.getOrElse("tokenGramSelection", tokenGramSelection.toString).toBoolean
    textGramSelection = paramMap.getOrElse("textGramSelection", textGramSelection.toString).toBoolean
    tokenSuffixSelection = paramMap.getOrElse("tokenSuffixSelection", tokenSuffixSelection.toString).toBoolean
    pcaSelection = paramMap.getOrElse("pcaSelection", pcaSelection.toString).toBoolean
    pcaK = paramMap.getOrElse("pcaK", pcaK.toString).toInt
    chiSquareSelection = paramMap.getOrElse("chiSquareSelection", chiSquareSelection.toString).toBoolean
    chiSquareBinSize = paramMap.getOrElse("chiSquareBinSize", chiSquareBinSize.toString).toInt
    chiK = paramMap.getOrElse("chiK", chiK.toString).toInt
  }

  override def hashCode(): Int = {
    var result = 1
    result = 7 * result + (if (termSelection) 1 else 0)
    result = 7 * result + (if (stemSelection) 1 else 0)
    result = 7 * result + (if (tokenGramSelection) 1 else 0)
    result = 7 * result + (if (textGramSelection) 1 else 0)
    result = 7 * result + (if (tokenSuffixSelection) 1 else 0)
    result = 7 * result + (if (pcaSelection) 1 else 0)
    result = 7 * result + (if (pcaSelection) pcaK else 0)
    result = 7 * result + (if (chiSquareSelection) 1 else 0)
    result = 7 * result + (if (chiSquareSelection) chiSquareBinSize else 0)
    result = 7 * result + (if (chiSquareSelection) chiK else 0)
    result
  }
}

object ParamsMorphology {
  lazy val analyzer: AnalyzerImp = new HasimAnalyzerImp(new EmptyTagModel)
  lazy val tokenizer: TokenizerImp = new MyTokenizer
  lazy val disambiguator = new MorphTagger()
}

object ParamsPostagger {
  lazy val posTagger: PosTaggerImp = new PosTaggerCRF(PennPosTagSet, MorphologyResources.crfPennPosTag)
}

object ParamsWord2Vec{
  val name = "Word Vector Parameters"
  var vectorWV2Size = 300
  var minW2VTrainingCount = 10
  var maxW2VIteration = 100
  var winW2VSize = 6
  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")
    printWriter.append("Vector Size: " + vectorWV2Size + "\n")
    printWriter.append("Minimum Training Count: " + minW2VTrainingCount + "\n")
    printWriter.append("Maximum Iteration: " + maxW2VIteration + "\n")
    printWriter.append("Window Size: " + winW2VSize + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Vector Size", "vectorSize", vectorWV2Size.toString)
    ParamsWriter.appendAsXML(printWriter, "Maximum Iteration", "maxW2VIteration", maxW2VIteration.toString)
    ParamsWriter.appendAsXML(printWriter, "Minimum Training Count", "minW2VTrainingCount", minW2VTrainingCount.toString)
    ParamsWriter.appendAsXML(printWriter, "Window Size", "winW2VSize", winW2VSize.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set("vectorWV2Size","maxW2VIteration", "minW2VTrainingCount", "winW2VSize"))
  }

  def assign(paramMap: Map[String, String]): Unit = {
    vectorWV2Size = paramMap.getOrElse("vectorWV2Size", vectorWV2Size.toString).toInt
    maxW2VIteration = paramMap.getOrElse("maxW2VIteration", maxW2VIteration.toString).toInt
    minW2VTrainingCount = paramMap.getOrElse("minW2VTrainingCount", minW2VTrainingCount.toString).toInt
    winW2VSize = paramMap.getOrElse("winW2VSize", winW2VSize.toString).toInt
  }

  override def hashCode(): Int = {
    var result = 1
    result = 7 * result + vectorWV2Size
    result = 7 * result + maxW2VIteration
    result = 7 * result + minW2VTrainingCount
    result = 7 * result + winW2VSize
    result
  }
}

object ParamsFunctionWords {
  val name = "Common and Function Word Parameters"
  var topCommonFunctionPOSTag = 5

  def append(printWriter: PrintWriter): Unit = {
    printWriter.append("===" + name + "===\n")
    printWriter.append("Minimum Common Function POS Frequency Threshold: " + topCommonFunctionPOSTag + "\n")
  }

  def appendAsXML(printWriter: PrintWriter): Unit = {
    printWriter.append("<PARAMETERS LABEL=\"" + name + "\">\n")
    ParamsWriter.appendAsXML(printWriter, "Minimum Common Function POS Frequency Threshold", "topCommonFunctionPOSTag", topCommonFunctionPOSTag.toString)
    printWriter.append("</PARAMETERS>\n")
  }

  def map(): Map[String, Set[String]] = {
    Map(name -> Set("topCommonFunctionPOSTag"))
  }

  def assign(paramMap: Map[String, String]): Unit = {
    topCommonFunctionPOSTag = paramMap.getOrElse("topCommonFunctionPOSTag", topCommonFunctionPOSTag.toString).toInt
  }

  override def hashCode(): Int = {
    var result = 1
    result = 7 * result + topCommonFunctionPOSTag
    result
  }

}

object ParamsWriter {


  def writeAsXML(filename: String): Unit = {
    val writer = new PrintWriter(filename)

    writer.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
    writer.write("<ROOT>\n")
    appendAsXML(writer)
    writer.write("</ROOT>")
    writer.close()
  }

  def append(printWriter: PrintWriter): Unit = {
    Parameters.append(printWriter)
    ParamsClassification.append(printWriter)
    ParamsEvaluation.append(printWriter)
    ParamsFeatures.append(printWriter)
    ParamsFunctionWords.append(printWriter)
    ParamsLength.append(printWriter)
    ParamsSample.append(printWriter)
    ParamsSentenceChar.append(printWriter)
    ParamsTokenChar.append(printWriter)
    ParamsTokenGram.append(printWriter)
    ParamsLanguageModel.append(printWriter)
    ParamsWeighting.append(printWriter)
    ParamsSelection.append(printWriter)
    ParamsLanguageModel.append(printWriter)
    ParamsWord2Vec.append(printWriter)


  }

  def appendAsXML(printWriter: PrintWriter): Unit = {

    Parameters.appendAsXML(printWriter)
    ParamsSample.appendAsXML(printWriter)
    ParamsFeatures.appendAsXML(printWriter)
    ParamsClassification.appendAsXML(printWriter)
    ParamsEvaluation.appendAsXML(printWriter)

    ParamsFunctionWords.appendAsXML(printWriter)
    ParamsLength.appendAsXML(printWriter)

    ParamsLanguageModel.appendAsXML(printWriter)
    ParamsLDAModel.appendAsXML(printWriter)
    ParamsWord2Vec.appendAsXML(printWriter)

    ParamsSentenceChar.appendAsXML(printWriter)
    ParamsTokenChar.appendAsXML(printWriter)
    ParamsTokenGram.appendAsXML(printWriter)
    ParamsWeighting.appendAsXML(printWriter)
    ParamsSelection.appendAsXML(printWriter)

  }

  def appendAsXML(printWriter: PrintWriter, description: String, tag: String, value: String): Unit = {
    printWriter.append("<param>\n")
    printWriter.append("<description>" + description + "</description>\n")
    printWriter.append("<tag>" + tag + "</tag>\n")
    printWriter.append("<value>" + value + "</value>\n")
    printWriter.append("</param>\n")
  }


}

object ParamsUnique {
  def evalID(): Int = {
    var result = 1
    result = 7 * result + Parameters.hashCode()
    result = 7 * result + ParamsClassification.hashCode()
    result = 7 * result + ParamsEvaluation.hashCode()
    result = 7 * result + ParamsFeatures.hashCode()
    result = 7 * result + ParamsFunctionWords.hashCode()
    result = 7 * result + ParamsLength.hashCode()
    result = 7 * result + ParamsSample.hashCode()
    result = 7 * result + ParamsSentenceChar.hashCode()
    result = 7 * result + ParamsTokenChar.hashCode()
    result = 7 * result + ParamsTokenGram.hashCode()
    result = 7 * result + ParamsLanguageModel.hashCode()
    result = 7 * result + ParamsLDAModel.hashCode()

    result = 7 * result + ParamsWeighting.hashCode()
    result = 7 * result + ParamsSelection.hashCode()
    result
  }

  def weightingID(): Int = {
    ParamsWeighting.hashCode()
  }

  def featuresID(): Int = {
    ParamsFeatures.hashCode()
  }

  def classificationID(): Int = {
    ParamsClassification.hashCode()
  }

  def sampleRDDID(): Int = {
    var result = 1
    result = 7 * result + Parameters.hashCode()
    result = 7 * result + ParamsSample.hashCode()
    result
  }

  def sampleProcessedID(): Int = {
    var result = 1
    result = 7 * result + Parameters.hashCode()
    result = 7 * result + ParamsSample.hashCode()
    result = 7 * result + ParamsFeatures.hashCode()
    result = 7 * result + ParamsFunctionWords.hashCode()
    result = 7 * result + ParamsLength.hashCode()
    result = 7 * result + ParamsSentenceChar.hashCode()
    result = 7 * result + ParamsTokenChar.hashCode()
    result = 7 * result + ParamsTokenGram.hashCode()
    result = 7 * result + ParamsLanguageModel.hashCode()
    result = 7 * result + ParamsLDAModel.hashCode()
    result = 7 * result + ParamsWord2Vec.hashCode()

    result = 7 * result + ParamsWeighting.hashCode()
    result = 7 * result + ParamsSelection.hashCode()
    result = 7 * result + ParamsEvaluation.hashCode()

    result
  }

  def checkConsistency(): Unit = {
    ParamsSample.checkConsistency()
    ParamsClassification.checkConsistency()

  }

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Weka Unique Filenames">

  def wekaARFFFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset.arff"
  }

  def wekaSampleFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset.sample"
  }

  def wekaSampleTrainFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-train.sample"
  }

  def wekaSampleTestFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-test.sample"
  }


  def wekaStatsFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset.stats"
  }

  def wekaStatsTrainFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-training.stats"
  }

  def wekaStatsTestFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-testing.stats"
  }

  def wekaARFFTrainFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-training.arff"
  }

  def wekaARFFTestFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.wekaARFFDir + processID.toString + "/dataset-testing.arff"
  }

  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Processed Dataframe Filenames">


  def processedTrainFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.processedDFDir + processID.toString + "/train-dataset"
  }

  def processedTestFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.processedDFDir + processID.toString + "/test-dataset"
  }

  def processedSampleFilename(): String = {
    val processID = sampleProcessedID()
    DatasetResources.processedDFDir + processID.toString + "/dataset.sample"
  }

  def processedStatsFilename(prefix: String): String = {
    val processID = sampleProcessedID()
    DatasetResources.processedDFDir + processID.toString + "/" + prefix + ".stats"
  }


  //</editor-fold>
  ///////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  //<editor-fold defaultstate="collapsed" desc="Sampled RDD/DF Filenames">
  def sampledRDDFilename(): String = {
    DatasetResources.sampledRDDDir + ParamsUnique.sampleRDDID() + "/dataset"
  }

  def sampledDFFilename(): String = {
    DatasetResources.sampledDFDir + ParamsUnique.sampleRDDID() + "/dataset"
  }

  def statisticsDFFilename(): String = {
    DatasetResources.sampledDFDir + ParamsUnique.sampleRDDID() + "/stats.xml"
  }

  //</editor-fold>


}

object ParamsReader {
  def readAsXML(filename: String): Unit = {
    val paramMap = XMLParser.parseParameters(filename)
    Parameters.assign(paramMap)
    ParamsClassification.assign(paramMap)
    ParamsEvaluation.assign(paramMap)
    ParamsFeatures.assign(paramMap)
    ParamsFunctionWords.assign(paramMap)
    ParamsLength.assign(paramMap)
    ParamsSample.assign(paramMap)
    ParamsSentenceChar.assign(paramMap)
    ParamsTokenChar.assign(paramMap)
    ParamsTokenGram.assign(paramMap)
    ParamsWeighting.assign(paramMap)
    ParamsSelection.assign(paramMap)
    ParamsLanguageModel.assign(paramMap)
    ParamsLDAModel.assign(paramMap)
    ParamsWord2Vec.assign(paramMap)
  }

  def readAsMap(filename: String): Map[String, String] = {
    XMLParser.parseParameters(filename)
  }

}

object ParamsTest {
  def main(args: Array[String]) {
    ParamsWriter.writeAsXML("params.xml")

    ParamsReader.readAsXML("params.xml")
    println(ParamsEvaluation.trainTestSplit.map(d => d.toString).mkString("|"))
  }
}



