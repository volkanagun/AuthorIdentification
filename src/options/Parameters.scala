package options

import java.util.Locale

import language.morphology.{EmptyTagModel, HasimAnalyzerImp}
import language.tokenization.MyTokenizer

/**
  * Created by wolf on 17.04.2016.
  */
object Parameters {

  val trLocale = new Locale("tr")
  val enLocale = new Locale("en")
  var crrLocale = new Locale("tr")


}

object ParamsSentenceChar{
  var maxCharGramLength = 8
  var minCharGramLength = 3
}

object ParamsTokenChar{
  var maxSuffixLength = 3
  var minSuffixLength = 1
  var maxPrefixLength = 5
  var minPrefixLength = 3
}

object ParamsTokenGram{
  var maxTokenGramLength = 3
  var minTokenGramLength = 2

  var maxPOSGramLength = 3
  var minPOSGramLength = 2
}

object ParamsMorphology{
  lazy val analyzer =  new HasimAnalyzerImp(new EmptyTagModel)
  lazy val tokenizer = new MyTokenizer

}

