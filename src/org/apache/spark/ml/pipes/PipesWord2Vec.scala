package org.apache.spark.ml.pipes

import java.io.File

import options.Parameters
import options.Resources.DocumentResources
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{BLAS, VectorUDT, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction, functions, types}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructType}

/**
  * Created by wolf on 20.09.2016.
  */

trait PipesWord2VecBase extends Params with HasInputCol with HasOutputCol {
  val typeMerging = new Param[String](this, "mergingType", "How to merge tokens to form a vector")
  val vectorSize: IntParam = new IntParam(this, "word2vec-size", "Word vector size")

  /**
    * Expects Seq[Sentence]
    *
    * @param value
    * @return
    */
  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setMergingType(value: String): this.type = set(typeMerging, value)

  def setVectorSize(value: Int): this.type = set(vectorSize, value)
}

class PipesWord2Vec(override val uid: String) extends Estimator[PipesWord2VecModel] with PipesWord2VecBase {
  //TODO: Load arbitrary word vectors from txt file

  def this() = this(Identifiable.randomUID("word2vec"))

  val modelFilename: Param[String] = new Param[String](this, "word2vec-model", "Word2Vec Model Filename")

  val minTrainingCount: IntParam = new IntParam(this, "minCount", "Minimum occurence of words for training")
  val maxIter: IntParam = new IntParam(this, "maxIter", "Maximum number of iterations")
  val winSize: IntParam = new IntParam(this, "winSize", "Window size")
  val buildModel: BooleanParam = new BooleanParam(this, "buildModel", "Force building model")


  def setModelFilename(value: String): this.type = set(modelFilename, value)

  def setMinTrainingCount(value: Int): this.type = set(minTrainingCount, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setWinSize(value: Int): this.type = set(winSize, value)

  def setBuildModel(value: Boolean): this.type = set(buildModel, value)


  protected def exists(filename: String): Boolean = {
    new File(filename).exists()
  }

  override def fit(dataset: DataFrame): PipesWord2VecModel = {
    val sc = SparkContext.getOrCreate()


    val w2vModel = if (exists($(modelFilename)) && !$(buildModel)) {
      //load model
      Word2VecModel.load(sc, $(modelFilename))
    }
    else {

      val rdd = dataset.select($(inputCol))
        .map(row => row.getAs[Seq[String]](0))
        .map(tokens => tokens.map(token => token.toLowerCase(Parameters.crrLocale)))
      val wmodel = new Word2Vec()
        .setVectorSize($(vectorSize))
        .setMinCount($(minTrainingCount))
        .setNumIterations($(maxIter))
        .setWindowSize($(winSize))
        .fit(rdd)

      wmodel.save(sc, $(modelFilename))
      wmodel
    }

    new PipesWord2VecModel(w2vModel)
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setVectorSize($(vectorSize))
      .setMergingType($(typeMerging))
      .setParent(this)

  }

  override def copy(extra: ParamMap): Estimator[PipesWord2VecModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val typeCandidates = List(ArrayType(types.StringType, true),
      ArrayType(types.StringType, false),
      ArrayType(ArrayType(StringType, true), true))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

class PipesWord2VecModel(override val uid: String, model: Word2VecModel) extends Model[PipesWord2VecModel] with PipesWord2VecBase {


  def this(model: Word2VecModel) = this(Identifiable.randomUID("word2vec-model"), model)

  accessQuality()


  override def copy(extra: ParamMap): PipesWord2VecModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    println("Vocabulary size of " + model.wordIndex.size)
    if (PipesWord2VecMerging.documentAveraging.equals($(typeMerging))) {
      documentAveraging(dataset)
    }
    else if(PipesWord2VecMerging.directVocabulary.equals($(typeMerging))){
      directVocabulary(dataset)
    }
    else{
      throw new Exception("Please specify extraction (merging) type exactly...")
    }
  }

  protected def accessQuality():Unit={
    //Take 5 words from dictionary find similar 10
    model.wordIndex.take(10).foreach(pair=>{
      val word=pair._1
      val index= pair._2
      val synonyms = model.findSynonyms(word, 10).mkString("[",",","]")
      println(word + " ==> "+synonyms)
    })
  }

  protected def createSparse(index: Int, mainIndice: Array[Int], mainArray: Array[Double],
                             appendArray: Array[Double]): (Int, Array[Int], Array[Double]) = {


    var newArray = mainArray ++ appendArray
    var newIndice = mainIndice ++ Range(0, appendArray.length).map(indice => indice + index)
    var newIndex = index + appendArray.length

    (newIndex, newIndice, newArray)
  }



  //<editor-fold desc="Document Averaging">

  protected def directVocabulary(dataset: DataFrame): DataFrame = {
    val vectors = model.getVectors
      .mapValues(vv => Vectors.dense(vv.map(_.toDouble)))
      .map(identity) // mapValues doesn't return a serializable map (SI-7005)
    val bVectors = dataset.sqlContext.sparkContext.broadcast(vectors)
    val d = $(vectorSize)



    val word2Vec = udf { document: Seq[Seq[String]] =>
      if (document.size == 0) {
        Vectors.sparse(d, Array.empty[Int], Array.empty[Double])
      } else {

        val tokens = document.flatten
          .distinct
          .map(token => token.toLowerCase(Parameters.crrLocale))

        //TODO: Create sparse array do it correctly.

        val vVectors = bVectors.value
        val empty = (0, Array[Int](), Array[Double]())
        val sparse = vVectors.foldLeft[(Int, Array[Int], Array[Double])](empty) {
          case (mainArr, tokenVec) => {

            val token = tokenVec._1
            val vector = tokenVec._2
            val index = mainArr._1
            val indices = mainArr._2
            val values = mainArr._3

            if (tokens.contains(token)) {
              createSparse(index,indices, values, vector.toArray)
            }
            else{
              (index + $(vectorSize), indices, values)
            }
          }
        }

        /*val array = vVectors.flatMap{case(token, value)=>{
          if(tokens.contains(token)){
            value.toArray
          }
          else{
            Array.fill[Double](d)(0.0)
          }
        }}.toArray[Double]*/

        //println("Feature size of " + sparse._1)
        Vectors.sparse(sparse._1, sparse._2, sparse._3)

      }
    }

    dataset.withColumn($(outputCol), word2Vec(col($(inputCol))))
  }

  protected def documentAveraging(dataset: DataFrame): DataFrame = {

    transformSchema(dataset.schema, logging = true)

    val vectors = model.getVectors
      .mapValues(vv => Vectors.dense(vv.map(_.toDouble)))
      .map(identity) // mapValues doesn't return a serializable map (SI-7005)
    val bVectors = dataset.sqlContext.sparkContext.broadcast(vectors)
    val d = $(vectorSize)
    val word2Vec = udf { document: Seq[Seq[String]] =>
      if (document.size == 0) {
        Vectors.sparse(d, Array.empty[Int], Array.empty[Double])
      } else {
        val sum = Vectors.zeros(d)

        document.foreach(sentence => {
          val inVocab = sentence.filter(token => bVectors.value.contains(token))
          inVocab.foreach { word =>
            bVectors.value.get(word).foreach { v =>
              BLAS.axpy(1.0, v, sum)
            }
          }
          BLAS.scal(1.0 / sentence.size, sum)
        })

        sum
      }
    }
    dataset.withColumn($(outputCol), word2Vec(col($(inputCol))))

  }

  //</editor-fold>

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val typeCandidates = List(ArrayType(ArrayType(types.StringType, true)), ArrayType(ArrayType(types.StringType, false)))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

object PipesWord2VecMerging {
  val documentAveraging = "DocumentAveraging"
  val directVocabulary = "DirectVocabulary"

}

