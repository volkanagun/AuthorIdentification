package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.extraction.{EmoticonDetector, _}
import org.apache.spark.ml.feature.reweighting.{IDFClass, OddsRatio}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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
      (0, Seq(Seq("Hi", "I", "heard", "about", "Spark", "."))),
      (1, Seq(Seq("Logistic", "regression", "models", "are", "neat", ".")))
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

  def wordlengths(sqlContext: SQLContext): Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I", "heard", "about", "Spark"))),
      (1, Seq(Seq("Logistic", "regression", ":-|", "models", "are", "neat", "??"), Seq(":-)")))
    )).toDF("label", "sentences")

    val wordlengths = new WordLengthCount()
    wordlengths.setMaxWordLength(15)
    wordlengths.setMinWordLength(1)
    wordlengths.setInputCol("sentences")
    wordlengths.setOutputCol("wordlength")

    wordlengths.transform(sentenceData).select("wordlength").show(false);

  }

  def wordforms(sqlContext: SQLContext): Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I", "heard", "about", "Spark"))),
      (1, Seq(Seq("Logistic", "regression", ":-|", "MODELS", "are", "neat", "??"), Seq("another", "lowercase", "start")))
    )).toDF("label", "sentences")

    val wordforms = new WordForms()

    wordforms.setInputCol("sentences")
    wordforms.setOutputCol("wordforms")
    wordforms.transform(sentenceData).select("wordforms").show(false)

  }

  def lengths(sqlContext: SQLContext): Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I", "heard", "about", "the", "Spark", "and", "the", "Scala"))),
      (1, Seq(Seq("Logistic", "regression", ":-|", "MODELS", "are", "neat", "??"), Seq("another", "lowercase", "start")))
    )).toDF("label", "sentences")

    val lengths = new LengthTokenLevel()

    lengths.setInputCol("sentences")
    lengths.setOutputCol("length")

    val outframe = lengths.transform(sentenceData)

  }

  def assembler(sqlContext: SQLContext): Unit = {
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I", "heard", "about", "the", "Spark", "and", "the", "Scala"))),
      (1, Seq(Seq("Logistic", "regression", ":-|", "MODELS", "are", "neat", "??"), Seq("another", "lowercase", "start")))
    )).toDF("label", "sentences")

    val lengths = new LengthTokenLevel()
    val wordlengths = new WordLengthCount()
    val vectorassembler = new VectorAssembler()

    lengths.setInputCol("sentences")
    lengths.setOutputCol("sentence-lengths")

    wordlengths.setInputCol("sentences")
    wordlengths.setOutputCol("word-lengths")

    vectorassembler.setInputCols(Array("sentence-lengths", "word-lengths"))
    vectorassembler.setOutputCol("combined-lengths")

    val senframe = lengths.transform(sentenceData)
    val wordframe = wordlengths.transform(senframe)
    val frameout = vectorassembler.transform(wordframe)

    show(frameout, vectorassembler.getOutputCol)


  }

  def slicer(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val data = Array(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(-2.0, 2.3, 0.0)
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val arr = StructType(Array(attrGroup.toStructField()))
    val dataRDD = sc.parallelize(data).map(Row.apply(_))
    val dataset = sqlContext.createDataFrame(dataRDD, arr)

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)

    println(output.select("userFeatures", "features").first())
  }

  def stopwordremove(sqlContext: SQLContext): Unit = {

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, Seq(Seq("I", "heard", "about", "the", "Spark", "and", "the", "Scala"))),
      (1, Seq(Seq("Logistic", "regression", ":-|", "MODELS", "are", "neat", "??"), Seq("another", "lowercase", "start")))
    )).toDF("label", "sentences")

    val stopwords = new StopWordRemover()

    stopwords.setInputCol("sentences")
    stopwords.setOutputCol("non-stop")
    stopwords.transform(sentenceData).select("non-stop").show(false);

  }

  def reweightingMatrix(sqlContext: SQLContext): Unit = {
    val tfdata = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(Array(3d, 0d, 7d))),
      (1.0, Vectors.dense(Array(0d, 0d, 7d))),
      (0.0, Vectors.dense(Array(1d, 4d, 2d))),
      (0.0, Vectors.dense(Array(0d, 1d, 2d))),
      (0.0, Vectors.dense(Array(0d, 1d, 1d))),
      (3.0, Vectors.dense(Array(0d, 0d, 1d))),
      (3.0, Vectors.dense(Array(1d, 0d, 1d))),
      (3.0, Vectors.dense(Array(0d, 1d, 1d)))
    )).toDF("label", "tfscores")

    val idfClass = new IDFClass()
    idfClass.setLabelCol("label")
    idfClass.setInputCol("tfscores")
    idfClass.setOutputCol("idfclassvector")
    val schema = new OddsRatio()
      .setTermNormalization(true)
      .setWeightNormalization(true)

    idfClass.setWeightingModel(schema)


    val idfClassModel = idfClass.fit(tfdata)
    idfClassModel.transform(tfdata).show(false)

  }

  def sclocal: SparkContext = {
    val conf = new SparkConf().setAppName("test").setMaster("local[1]");
    new SparkContext(conf);
  }

  def local(sparkContext: SparkContext): SQLContext = {
    return new SQLContext(sparkContext);
  }

  def show(dataset: DataFrame, col: String): Unit = {
    val schema = dataset.schema
    val column = schema.apply(col)
    val attributes = AttributeGroup.fromStructField(column)
    val size = attributes.size
    for (i <- 0 until size) {
      val name = attributes.getAttr(i).name
      name match {
        case Some(value) => println(value + " ")
        case None => {}
      }

    }
  }

}

object Main {
  def main(args: Array[String]): Unit = {
    val test = new PipelineTests
    val sc = test.sclocal
    val sqlContext = test.local(sc)
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
    test.assembler(sqlContext)
    //test.slicer(sc,sqlContext)
    //test.reweightingMatrix(sqlContext)
  }
}
