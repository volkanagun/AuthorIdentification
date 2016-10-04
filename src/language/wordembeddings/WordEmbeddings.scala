
package language.wordembeddings

import java.io.{BufferedOutputStream, File, FileOutputStream, FileWriter, PrintWriter}
import java.nio.IntBuffer
import java.util

import breeze.linalg.{DenseVector, Vector}
import language.morphology.MorphTagger
import language.tokenization.MyTokenizer
import options.Parameters
import options.Resources.{DocumentResources, Word2VecResources}
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.models.word2vec.Word2Vec.Builder
import org.deeplearning4j.text.sentenceiterator.{BasicLineIterator, SentenceIterator}
import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory, UimaTokenizerFactory}
import org.nd4j.linalg.api.buffer.DataBuffer
import org.nd4j.linalg.api.complex.{IComplexNDArray, IComplexNumber}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.conditions.Condition
import org.nd4j.linalg.indexing.{INDArrayIndex, ShapeOffsetResolution}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

import scala.io.Source
import scala.util.control.Breaks

/**
  * Created by Administrator on 9/23/2016.
  **/
object VecUtil extends Serializable{

  def sum(arr:Array[Double], narr:Array[Double]):Array[Double]={
    val vec = new DenseVector[Double](arr)
    val nvec = new DenseVector[Double](narr)
    nvec.+(vec).toArray
  }

  def sumAvg(arr:Array[Double], narr:Array[Double]):Array[Double]={
    val vec = new DenseVector[Double](arr)
    val nvec = new DenseVector[Double](narr)
    (nvec.:+(vec)).:/(2d).toArray

  }

  def sumAvg(words: Array[Array[Double]], vecSize:Int): Array[Double] = {
    words.foldLeft[DenseVector[Double]](DenseVector.zeros[Double](vecSize)) { case (source, word) => {
       val vector = new DenseVector(word)
       (source.+(vector))
    }
    }.:/(words.length.toDouble).toArray
  }

  def sum(words: Array[Array[Double]], vecSize:Int): Array[Double] = {
    words.foldLeft[DenseVector[Double]](DenseVector.zeros[Double](vecSize)) { case (source, word) => {
       val vector = new DenseVector(word)
       (source.+(vector))
    }
    }.toArray
  }

  def cosineSimilarity(src:Array[Double], dst:Array[Double]):Double={
    require(src.length == dst.length, "Word vector sizes must be equal...")
    val tuple = src.zip(dst).foldLeft[(Double, Double, Double)]((0d, 0d, 0d)) { case (sum, (d1, d2)) => {
      val dotSum = sum._1 + d1 * d2
      val norm1 = sum._2 + d1 * d1
      val norm2 = sum._3 + d2 * d2
      (dotSum, norm1, norm2)
    }
    }

    tuple._1 / (math.sqrt(tuple._2) * math.sqrt(tuple._3))
  }

  def similarity(average:Array[Double], items:Array[Array[Double]]):(Double, Array[Double]) = {

    items.foldLeft[(Double, Array[Double])]((1.0, average)){
      case((sim, avg), item)=>{
        val nsim = sim * VecUtil.cosineSimilarity(avg, item)
        val navg = VecUtil.sumAvg(avg, item)
        (nsim, navg)
      }
    }
  }
}

class WordEmbeddings extends Serializable {
  private val log: Logger = LoggerFactory.getLogger(classOf[WordEmbeddings])
  //Create word embeddings from sentence data
  //Disambiguate each word by morphological analyzer
  //Turn a word into stem tag tag format
  //Save and load disambiguated text
  val tagger = new MorphTagger()
  val tokenizer = new MyTokenizer()
  val locale = Parameters.crrLocale
  val windowSize = 25
  val vectorSize = 300
  val minFreq = 5
  val maxIter = 200
  val maxNearest = 5000

  lazy val word2vec = WordVectorSerializer.loadTxtVectors(new File(Word2VecResources.word2VecTxt))

  def normalizeQuoto(sentence: String): String = {
    val norm = sentence
    val length = "\"".r.findAllIn(norm).length

    if (length == 1) {
      "\"" + norm
    }
    else {
      norm
    }
  }



  def stemPOSEmbeddingDataPar(sentenceFilename: String, dataFilename: String): Unit = {
    val lines = Source.fromFile(new File(sentenceFilename)).getLines()
    val writer = new PrintWriter(dataFilename)
    log.info("Tokenization and tagging is started...")
    val parIter = lines.sliding(24, 24)

    parIter.foreach(sentences => {
      var mappedData = sentences.par.map(sentence => normalizeQuoto(sentence))
      val appendData = mappedData.par.map(sentence => {
        println(s"Sentence: ${sentence}")
        val tokens = tokenizer.tokenize(sentence)
        val stemTags = tagger.predict(tokens)
        val datum = stemTags.par.flatMap(stemTag => {
          val tags = stemTag.split("\\+")
          val morphs = tags.slice(1, tags.length)
            .map(tag => tag.toUpperCase())
          val flat = Array(tags(0).toLowerCase(locale)) ++ morphs
          flat
        }).mkString(" ")

        datum
      })

      writer.write(appendData.mkString("\n") + "\n")

    })

    writer.close()
  }

  def tokenPOSEmbeddingDataPar(sentenceFilename: String, dataFilename: String): Unit = {
    val lines = Source.fromFile(new File(sentenceFilename)).getLines()
    val writer = new PrintWriter(new FileWriter(dataFilename, true))
    log.info("Tokenization and tagging is started...")
    val parIter = lines.sliding(24, 24)

    parIter.foreach(sentences => {
      var mappedData = sentences.par.map(sentence => normalizeQuoto(sentence))
      val appendData = mappedData.par.map(sentence => {
        println(s"Sentence: ${sentence}")
        val tokens = tokenizer.tokenize(sentence)
        val stemTags = tagger.predict(tokens)
        val datum = stemTags.zip(tokens).par.flatMap { case (stemTag, token) => {
          val tags = stemTag.split("\\+")
          val morphs = tags.slice(1, tags.length)
            .map(tag => tag.toUpperCase())
          val flat = Array(token.toLowerCase(locale)) ++ morphs
          flat
        }
        }.mkString(" ")

        datum
      })

      writer.write(appendData.mkString("\n") + "\n")

    })

    writer.close()
  }

  def createEmbeddingData(sentenceFilename: String, dataFilename: String): Unit = {
    val lines = Source.fromFile(new File(sentenceFilename)).getLines()
    val writer = new PrintWriter(dataFilename)
    log.info("Tokenization and tagging is started...")
    while (lines.hasNext) {
      val sentence = lines.next()
      println(s"Sentence: ${sentence}")
      val tokens = tokenizer.tokenize(sentence)
      val stemTags = tagger.predict(tokens)
      val datum = stemTags.flatMap(stemTag => {
        val tags = stemTag.split("\\+")
        val morphs = tags.slice(1, tags.length)
          .map(tag => tag.toUpperCase(locale))
        val flat = Array(tags(0).toLowerCase(locale)) ++ morphs
        flat
      }).mkString(" ")

      writer.write(datum + "\n")
    }

    writer.close()
  }

  def createWord2Vec(dataFilename: String, word2vecFilename: String): Unit = {
    val filePath: String = new File(dataFilename).getAbsolutePath

    log.info("Load & Vectorize Sentences....")
    // Strip white space before and after for each line
    val iter: SentenceIterator = new BasicLineIterator(filePath)
    // Split on white spaces in the line to get words
    val t: TokenizerFactory = new DefaultTokenizerFactory()


    log.info("Building model....")
    val builder = new Word2Vec.Builder()
    val vec: Word2Vec = builder.minWordFrequency(minFreq)
      .iterations(maxIter)
      .layerSize(vectorSize)
      .seed(42)
      .windowSize(windowSize)
      .iterate(iter)
      .tokenizerFactory(t).build

    log.info("Fitting Word2Vec model....")
    vec.fit

    log.info("Writing word vectors to text file....")

    // Write word vectors
    WordVectorSerializer.writeWordVectors(vec, word2vecFilename)
  }


  def vectors(items:Array[String]) : Array[(Array[Double],Int)]={
    items.zipWithIndex.map{case(item, indice)=>{
      (word2vec.getWordVector(item), indice)
    }}.filter{case(item, indice)=> item!=null}
  }

  def vectorsExists(items:Array[String]):Array[Array[Double]]={
    items.map(item=> word2vec.getWordVector(item)).filter(p=>p!=null)
  }


  protected def process(line: String): java.util.Iterator[String] = {
    if (line.contains("+") && !line.contains("=")) {
      val words = line.split("\\s?\\+\\s?")
      val vector = VecUtil.sumAvg(vectorsExists(words), vectorSize)

      val array = Nd4j.create(vector)
      word2vec.wordsNearest(array, maxNearest).iterator()

    }
    else if (line.contains("=")) {
      val equality: Array[String] = line.split("\\s?\\=\\s?")
      val positive: String = equality(0)
      val negative: String = equality(1)
      val positives = positive.split("\\s?\\+\\s?")
      val negatives = negative.split("\\s?\\+\\s?")
      val positiveCol = util.Arrays.asList(positives: _*)
      val negativeCol = util.Arrays.asList(negatives: _*)

      word2vec.wordsNearest(positiveCol, negativeCol, maxNearest)
        .iterator()

    }
    else {
      word2vec.wordsNearest(line, maxNearest).iterator()
    }
  }

  def test(filename: String): Unit = {
    val reg = "^[A-ZÇÖÜĞŞİ\\^\\-]+$"
    Breaks.breakable {
      while (true) {
        println("Enter Word or Exit (E):")
        val word = Console.readLine().trim
        if (word.equals("E")) Breaks.break()
        else {
          val pline = process(word)
            .toArray.filter(w=> w.matches(reg))
            .mkString("[",",","]")
          println(pline)
          println()
        }
      }
    }
  }

}

object WordEmbeddings extends WordEmbeddings {

  val smallFilename = DocumentResources.smallSentences
  val largeFilename = DocumentResources.largeSentences
  val word2vecTxt = Word2VecResources.word2VecTxt


  def main(args: Array[String]) {
    println("Create Large or Small Embeddings or Test (L/S/T)")
    val answerLorS = Console.readLine().trim
    if (answerLorS.equals("L")) {
      println("Stem-POS or Token-POS (S/T)")
      val answerSorT = Console.readLine().trim
      if (answerSorT.equals("S")) {
        println("Create data file? (Y/N)")
        val answerC = Console.readLine().trim
        if (answerC.equals("Y")) {
          stemPOSEmbeddingDataPar(largeFilename, DocumentResources.embeddingLargeStemSentences)
        }

        createWord2Vec(DocumentResources.embeddingLargeStemSentences, word2vecTxt)
      }
      else if (answerSorT.equals("T")) {
        println("Create data file? (Y/N)")
        val answerC = Console.readLine().trim
        if (answerC.equals("Y")) {
          tokenPOSEmbeddingDataPar(largeFilename, DocumentResources.embeddingLargeTokenSentences)
        }

        createWord2Vec(DocumentResources.embeddingLargeTokenSentences, word2vecTxt)
      }

    }
    else if (answerLorS.equals("S")) {
      println("Stem-POS or Token-POS (S/T)")
      val answerSorT = Console.readLine().trim
      if (answerSorT.equals("S")) {
        println("Create data file? (Y/N)")
        val answerC = Console.readLine().trim
        if(answerC.equals("Y")) {
          stemPOSEmbeddingDataPar(smallFilename, DocumentResources.embeddingSmallStemSentences)
        }

        createWord2Vec(DocumentResources.embeddingSmallStemSentences, word2vecTxt)
      }
      else if (answerSorT.equals("T")) {
        println("Create data file? (Y/N)")
        val answerC = Console.readLine().trim
        if(answerC.equals("Y")) {
          tokenPOSEmbeddingDataPar(smallFilename, DocumentResources.embeddingSmallTokenSentences)
        }

        createWord2Vec(DocumentResources.embeddingSmallTokenSentences, word2vecTxt)
      }

    }
    else if (answerLorS.equals("T")) {
      test(word2vecTxt)
    }

  }
}

