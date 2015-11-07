package org.apache.spark.ml.feature

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wolf on 27.10.2015.
 */
class PipelineTests {
  def ngrams(sqlContext: SQLContext): Unit = {
    val wordDataFrame = sqlContext.createDataFrame(
      Seq(
        (0, Array("Hi", "I", "heard", "about", "Spark")),
        (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
        (2, Array("Logistic", "regression", "models", "are", "neat"))
      )).toDF("label", "words");

    val ngram: NGram = new NGram().setN(5).setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
  }

  def sentences(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark. I wish Java could use case classes"),
      (1, "Logistic regression models are neat.")
    )).toDF("label", "text")
    val detector = new OpenSentenceDetector();
    detector.setInputCol("text")
    detector.setOutputCol("sentences")

    detector.transform(sentenceData).select("sentences").show(false)
  }

  def tokenizer(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq("Hi I heard about Spark.", "I wish Java could use case classes")),
      (1, Seq("Logistic regression models are neat."))
    )).toDF("label", "sentences")
    val tokenizer = new OpenTokenizer();
    tokenizer.setInputCol("sentences")
    tokenizer.setOutputCol("tokens")

    tokenizer.transform(sentenceData).select("tokens").show(false)
  }

  def charngrams(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq("Hi I heard about Spark.")),
      (1, Seq("Logistic regression models are neat."))
    )).toDF("label", "sentences")

    val ngrams = new NGramChars();
    ngrams.setMax(3)
    ngrams.setMin(3)
    ngrams.setInputCol("sentences")
    ngrams.setOutputCol("chars")

    ngrams.transform(sentenceData).select("chars").show(false)
  }

  def wordngrams(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("Hi", "I", "heard","about", "Spark","."))),
      (1, Seq(Seq("Logistic","regression","models", "are", "neat",".")))
    )).toDF("label", "sentences")

    val wordgrams = new NGramWords();
    wordgrams.setMax(5)
    wordgrams.setMin(2)
    wordgrams.setInputCol("sentences")
    wordgrams.setOutputCol("word-grams")

    wordgrams.transform(sentenceData).select("word-grams").show(false)
  }

  def puncsrepeat(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq("Hi I heard about Spark...")),
      (1, Seq("Logistic regression \"\" '' [[ (( %% && @@ models are neat??", ":-)"))
    )).toDF("label", "sentences")

    val puncs = new RepeatedPuncs()

    puncs.setInputCol("sentences")
    puncs.setOutputCol("puncs")

    puncs.transform(sentenceData).select("puncs").show(false)
  }

  def emoticons(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq("I heard about Spark")),
      (1, Seq("Logistic regression :-| models are neat??", ":-)"))
    )).toDF("label", "sentences")

    val emoticons = new EmoticonDetector()

    emoticons.setInputCol("sentences")
    emoticons.setOutputCol("emos")

    emoticons.transform(sentenceData).select("emos").show(false)
  }

  def wordlengths(sqlContext: SQLContext):Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I","heard","about","Spark"))),
      (1, Seq(Seq("Logistic","regression", ":-|", "models","are","neat","??"), Seq(":-)")))
    )).toDF("label", "sentences")

    val wordlengths = new WordLengthCount()
    wordlengths.setMaxWordLength(15)
    wordlengths.setMinWordLength(1)
    wordlengths.setInputCol("sentences")
    wordlengths.setOutputCol("wordlength")

    wordlengths.transform(sentenceData).select("wordlength").show(false);

  }

  def wordforms(sqlContext: SQLContext):Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I","heard","about","Spark"))),
      (1, Seq(Seq("Logistic","regression", ":-|", "MODELS","are","neat","??"), Seq("another","lowercase", "start")))
    )).toDF("label", "sentences")

    val wordforms = new WordForms()

    wordforms.setInputCol("sentences")
    wordforms.setOutputCol("wordforms")
    wordforms.transform(sentenceData).select("wordforms").show(false);

  }

  def stopwords(sqlContext: SQLContext):Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I","heard","about","the","Spark","and","the","Scala"))),
      (1, Seq(Seq("Logistic","regression", ":-|", "MODELS","are","neat","??"), Seq("another","lowercase", "start")))
    )).toDF("label", "sentences")

    val stopwords = new LengthTokenLevel()

    stopwords.setInputCol("sentences")
    stopwords.setOutputCol("stopwords")
    stopwords.transform(sentenceData).select("stopwords").show(false);

  }

  def stopwordremove(sqlContext: SQLContext):Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I","heard","about","the","Spark","and","the","Scala"))),
      (1, Seq(Seq("Logistic","regression", ":-|", "MODELS","are","neat","??"), Seq("another","lowercase", "start")))
    )).toDF("label", "sentences")

    val stopwords = new StopWordRemover()

    stopwords.setInputCol("sentences")
    stopwords.setOutputCol("non-stop")
    stopwords.transform(sentenceData).select("non-stop").show(false);

  }


  def local(): SQLContext = {
    val conf = new SparkConf().setAppName("test").setMaster("local[12]");
    val sc = new SparkContext(conf);
    return new SQLContext(sc);
  }

}

object Main {
  def main(args: Array[String]): Unit = {
    val test = new PipelineTests
    val sqlContext = test.local()
    //test.ngrams(sqlContext)
    //test.sentences(sqlContext)
    //test.tokenizer(sqlContext)
    //test.charngrams(sqlContext)
    //test.puncsrepeat(sqlContext)
    //test.emoticons(sqlContext)
    //test.wordlengths(sqlContext)
    //test.wordngrams(sqlContext)
    //test.wordforms(sqlContext)
    //test.stopwords(sqlContext)
    test.stopwordremove(sqlContext)
  }
}
