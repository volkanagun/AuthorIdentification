package language.tokenization

import java.io.Serializable
import java.util.regex.{Matcher, Pattern}

import com.aliasi.tokenizer
import com.aliasi.tokenizer.{Tokenizer, IndoEuropeanTokenizerFactory, TokenizerFactory}

/**
  * Created by wolf on 26.03.2016.
  */
class MyTokenizer extends TokenizerImp {
  private val tokenizerFactory: TokenizerFactory = IndoEuropeanTokenizerFactory.INSTANCE
  private var model: TokenizerModel = TokenizerModel.build

  private def normalize(sentence:String):String={
    sentence
    //sentence.replaceAll("[\'\\´\\`](\\s?)","")
  }

  private def pretokenize(sentence: String): Seq[MyToken] = {
    val chars: Array[Char] = sentence.toCharArray
    val tokenizer: com.aliasi.tokenizer.Tokenizer = tokenizerFactory.tokenizer(chars, 0, chars.length)
    var token: String = null
    var tokenList = Seq[MyToken]()
    while ((({
      token = tokenizer.nextToken;
      token
    })) != null) {
      val start: Int = tokenizer.lastTokenStartPosition
      val end: Int = tokenizer.lastTokenEndPosition
      tokenList = tokenList :+ (new MyToken(token, start, end))
    }
    return tokenList
  }

  def tokenize(sentence: String): Seq[String] = {
    val normalized = normalize(sentence)
    val knownList = model.matching(normalized)
    val tokenList = pretokenize(normalized)
    var resultList: Seq[String] = Seq[String]()
    var start: Int = 0

    for (token <- tokenList) {
      if (token.getStart >= start) {
        val indice = index(knownList, token, start)
        if (indice == -1) {
          resultList = resultList :+ token.getToken
          start = token.getEnd
        }
        else {
          val known = knownList(indice)
          resultList = resultList :+ (known.getToken)
          start = known.getEnd
        }
      }
    }

    resultList
  }

  def tokenizeAsArray(sentence:String) : Array[String]={
    tokenize(sentence).toArray
  }

  def index(knownList: Seq[MyToken], token: MyToken, start: Int): Int = {
    var searchList = Seq[MyToken]()
    for (tok <- knownList) {
      if (tok.getStart >= start) {
        searchList = searchList :+ token
      }
    }

    var i: Int = 0
    while (i < searchList.size) {
      {
        val known = knownList(i)
        if (known.getStart <= token.getStart && known.getEnd >= token.getEnd) {
          return i
        }
      }

      i += 1
    }

    return -1
  }
}

class MyToken extends Serializable {
  private var token: String = null
  private var start: Int = 0
  private var end: Int = 0

  def this(token: String, start: Int, end: Int) {
    this()
    this.token = token
    this.start = start
    this.end = end
  }

  def getToken: String = {
    return token
  }

  def getStart: Int = {
    return start
  }

  def getEnd: Int = {
    return end
  }

  override def toString: String = {
    return token
  }


  override def hashCode: Int = {
    var result: Int = getToken.hashCode
    result = 31 * result + getStart
    return result
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyToken]

  override def equals(other: Any): Boolean = other match {
    case that: MyToken =>
      (that canEqual this) &&
        token == that.token &&
        start == that.start
    case _ => false
  }
}

class TokenizerRegex extends Serializable {
  private var pattern: Pattern = null
  private var replace: String = null
  private var group: Int = 0

  def this(regex: String, replace: String, group: Int) {
    this()
    this.pattern = Pattern.compile(regex)
    this.replace = replace
    this.group = group
  }

  def this(regex: String, group: Int) {
    this()
    this.pattern = Pattern.compile(regex)
    this.group = group
    this.replace = "$" + String.valueOf(group)
  }

  def this(regex: String) {
    this(regex, "$1", 0)
  }

  def matching(text: String): Seq[MyToken] = {
    var tokenList = Seq[MyToken]()
    val matcher: Matcher = pattern.matcher(text)
    while (matcher.find) {
      val value: String = matcher.group(group)
      val start: Int = matcher.start(group)
      val end: Int = matcher.end(group)
      tokenList = tokenList :+ new MyToken(value, start, end)
    }
    return tokenList
  }
}



class TokenizerModel extends Serializable {
  private var regexList: Seq[TokenizerRegex] = Seq[TokenizerRegex]()

  def matching(text: String): Seq[MyToken] = {
    var tokens = Seq[MyToken]()

    for (reg <- regexList) {
      val matchs = reg.matching(text)
      tokens = tokens ++ matchs
    }
    return tokens
  }

  def add(regex: TokenizerRegex) {
    regexList = regexList :+ regex
  }
}

object TokenizerModel {
  def build: TokenizerModel = {
    val model: TokenizerModel = new TokenizerModel

    model.add(new TokenizerRegex("<([a-z]+)([^<]+)*(?:>(.*)<\\/\\1>|\\s+\\/>)"))
    model.add(new TokenizerRegex("(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?"))
    model.add(new TokenizerRegex("((https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])"))
    model.add(new TokenizerRegex("(([A-ZİĞÜŞÇÖ][a-zığüşçö]+)(\'[a-zığüşçö]+))", 3))


    return model
  }
}

object MyTokenizer{

  def main(args: Array[String]) {
    val tokenizer = new MyTokenizer
    tokenizer.tokenize("Kaynak'tan : aliveli.com").foreach(token=>println(token))
  }
}