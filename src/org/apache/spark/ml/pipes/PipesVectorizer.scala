/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.ml.pipes

import language.util.TextSlicer
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.pipes.PipeSliceVectorizerModel.PipeSliceVectorizerModelWriter
import org.apache.spark.ml.util._
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{SparseVector, VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
  * Params for [[PipeVectorizer]] and [[PipeVectorizerModel]].
  */
trait PipeVectorizerParams extends Params with HasInputCol with HasOutputCol {

  /**
    * Max size of the vocabulary.
    * CountVectorizer will build a vocabulary that only considers the top
    * vocabSize terms ordered by term frequency across the corpus.
    *
    * Default: 2^18^
    *
    * @group param
    */
  val vocabSize: IntParam =
  new IntParam(this, "vocabSize", "max size of the vocabulary", ParamValidators.gt(0))

  /** @group getParam */
  def getVocabSize: Int = $(vocabSize)

  /**
    * Specifies the minimum number of different documents a term must appear in to be included
    * in the vocabulary.
    * If this is an integer >= 1, this specifies the number of documents the term must appear in;
    * if this is a double in [0,1), then this specifies the fraction of documents.
    *
    * Default: 1
    *
    * @group param
    */
  val minDF: DoubleParam = new DoubleParam(this, "minDF", "Specifies the minimum number of" +
    " different documents a term must appear in to be included in the vocabulary." +
    " If this is an integer >= 1, this specifies the number of documents the term must" +
    " appear in; if this is a double in [0,1), then this specifies the fraction of documents.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinDF: Double = $(minDF)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val typeCandidates = List(ArrayType(StringType, true), ArrayType(StringType, false))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  protected def validateTransformSchema(schema: StructType, attrs: Array[String]): StructType = {
    var fields = schema.fields
    val newSchema = AttributeCreator.append(schema, $(outputCol), attrs)

    newSchema
  }

  /**
    * Filter to ignore rare words in a document. For each document, terms with
    * frequency/count less than the given threshold are ignored.
    * If this is an integer >= 1, then this specifies a count (of times the term must appear
    * in the document);
    * if this is a double in [0,1), then this specifies a fraction (out of the document's token
    * count).
    *
    * Note that the parameter is only used in transform of [[PipeVectorizerModel]] and does not
    * affect fitting.
    *
    * Default: 1
    *
    * @group param
    */
  val minTF: DoubleParam = new DoubleParam(this, "minTF", "Filter to ignore rare words in" +
    " a document. For each document, terms with frequency/count less than the given threshold are" +
    " ignored. If this is an integer >= 1, then this specifies a count (of times the term must" +
    " appear in the document); if this is a double in [0,1), then this specifies a fraction (out" +
    " of the document's token count). Note that the parameter is only used in transform of" +
    " CountVectorizerModel and does not affect fitting.", ParamValidators.gtEq(0.0))

  setDefault(minTF -> 1)

  /** @group getParam */
  def getMinTF: Double = $(minTF)
}

/**
  * :: Experimental ::
  * Extracts a vocabulary from document collections and generates a [[PipeVectorizerModel]].
  */
@Experimental
class PipeVectorizer(override val uid: String)
  extends Estimator[PipeVectorizerModel] with PipeVectorizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("cntVec"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  /** @group setParam */
  def setMinDF(value: Double): this.type = set(minDF, value)

  /** @group setParam */
  def setMinTF(value: Double): this.type = set(minTF, value)

  setDefault(vocabSize -> (1 << 18), minDF -> 1)

  override def fit(dataset: DataFrame): PipeVectorizerModel = {

    println("Building TF scores for " + $(inputCol) + " column")

    transformSchema(dataset.schema, logging = true)
    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).map(_.getAs[Seq[String]](0))
    val minDf = if ($(minDF) >= 1.0) {
      $(minDF)
    } else {
      $(minDF) * input.cache().count()
    }
    val wordCounts: RDD[(String, Long)] = input.flatMap { case (tokens) =>
      val wc = new OpenHashMap[String, Long]
      tokens.foreach { w =>
        wc.changeValue(w, 1L, _ + 1L)
      }
      wc.map { case (word, count) => (word, (count, 1)) }
    }.reduceByKey { case ((wc1, df1), (wc2, df2)) =>
      (wc1 + wc2, df1 + df2)
    }.filter { case (word, (wc, df)) =>
      df >= minDf
    }.map { case (word, (count, dfCount)) =>
      (word, count)
    }.cache()
    val fullVocabSize = wordCounts.count()
    val vocab: Array[String] = {
      val tmpSortedWC: Array[(String, Long)] = if (fullVocabSize <= vocSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocSize)
      }
      tmpSortedWC.map(_._1)
    }

    require(vocab.length > 0, "The vocabulary size should be > 0. Lower minDF as necessary.")
    println("Building TF Vocabulary model is finished...")


    copyValues(new PipeVectorizerModel(uid, vocab).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): PipeVectorizer = defaultCopy(extra)
}

@Since("1.6.0")
object PipeVectorizer extends DefaultParamsReadable[PipeVectorizer] {

  @Since("1.6.0")
  override def load(path: String): PipeVectorizer = super.load(path)
}

/**
  * :: Experimental ::
  * Converts a text document to a sparse vector of token counts.
  *
  * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
  */
@Experimental
class PipeVectorizerModel(override val uid: String, val vocabulary: Array[String])
  extends Model[PipeVectorizerModel] with PipeVectorizerParams with MLWritable {

  import PipeVectorizerModel._

  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("cntVecModel"), vocabulary)
    set(vocabSize, vocabulary.length)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMinTF(value: Double): this.type = set(minTF, value)

  /** Dictionary created from [[vocabulary]] and its indices, broadcast once for [[transform()]] */
  private var broadcastDict: Option[Broadcast[Map[String, Int]]] = None

  override def transform(dataset: DataFrame): DataFrame = {
    if (broadcastDict.isEmpty) {
      val dict = vocabulary.zipWithIndex.toMap
      broadcastDict = Some(dataset.sqlContext.sparkContext.broadcast(dict))
    }
    val dictBr = broadcastDict.get
    val minTf = $(minTF)
    val vectorizer = udf { (document: Seq[String]) =>
      val termCounts = new OpenHashMap[Int, Double]
      var tokenCount = 0L
      document.foreach { term =>
        dictBr.value.get(term) match {
          case Some(index) => termCounts.changeValue(index, 1.0, _ + 1.0)
          case None => // ignore terms not in the vocabulary
        }
        tokenCount += 1
      }
      val effectiveMinTF = if (minTf >= 1.0) {
        minTf
      } else {
        tokenCount * minTf
      }

      val filterCounts = termCounts.filter(_._2 >= effectiveMinTF).toSeq

      Vectors.sparse(dictBr.value.size, filterCounts)
    }

    val metadata = AttributeCreator.createFiled(vocabulary, $(outputCol))
    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))), metadata.metadata)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): PipeVectorizerModel = {
    val copied = new PipeVectorizerModel(uid, vocabulary).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PipeVectorizerModelWriter(this)
}

/**
  * Apply vectorizer with a known focabulary to slices to tokens
  *
  * @param uid
  * @param vocabulary
  */
@Experimental
class PipeSliceVectorizerModel(override val uid: String, val vocabulary: Array[String])
  extends Model[PipeSliceVectorizerModel] with PipeVectorizerParams with MLWritable {

  //import PipeVectorizerModel._

  val sliceNums = new IntParam(this, "num-slices", "number of slices")

  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("cntSliceVecModel"), vocabulary)
    set(vocabSize, vocabulary.length)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMinTF(value: Double): this.type = set(minTF, value)

  def setSliceNum(value: Int): this.type = set(sliceNums, value)

  /** Dictionary created from [[vocabulary]] and its indices, broadcast once for [[transform()]] */
  private var broadcastDict: Option[Broadcast[Map[String, Int]]] = None

  protected def vectorize(document: Seq[String], dictBr: Broadcast[Map[String, Int]]): mllib.linalg.Vector = {

    val termCounts = new OpenHashMap[Int, Double]
    var tokenCount = 0L
    document.foreach { term =>
      dictBr.value.get(term) match {
        case Some(index) => termCounts.changeValue(index, 1.0, _ + 1.0)
        case None => // ignore terms not in the vocabulary
      }
      tokenCount += 1
    }

    Vectors.sparse(dictBr.value.size, termCounts.filter(_._2 >= 1).toSeq)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    if (broadcastDict.isEmpty) {
      val dict = vocabulary.zipWithIndex.toMap
      broadcastDict = Some(dataset.sqlContext.sparkContext.broadcast(dict))
    }
    val dictBr = broadcastDict.get
    val minTf = 1
    val numSlices = $(sliceNums)
    val range = Range(0, numSlices)

    range.foldLeft[DataFrame](dataset) {
      case (df, index) => {
        val sliceUDF = functions.udf {
          sequence: Seq[Seq[String]] => {
            vectorize(sequence(index), dictBr)
          }
        }

        val outColName = $(outputCol) + "_" + index
        val metadata = AttributeCreator.createFiled(vocabulary, outColName)
        val updatedDataset = df.withColumn(outColName, sliceUDF(col($(inputCol))), metadata.metadata)

        updatedDataset
      }
    }

  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == ArrayType(ArrayType(StringType, false)), s"The input column $inputColName must be Array[String] type " +
      s"but got $inputDataType.")

    val sliceCnt = $(sliceNums)
    var mschema = schema
    for (i <- 0 until sliceCnt) {
      mschema = SchemaUtils.appendColumn(mschema, $(outputCol), new VectorUDT)
    }
    mschema
  }

  override def copy(extra: ParamMap): PipeSliceVectorizerModel = {
    val copied = new PipeSliceVectorizerModel(uid, vocabulary).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PipeSliceVectorizerModelWriter(this)
}


@Since("1.6.0")
object PipeVectorizerModel extends MLReadable[PipeVectorizerModel] {

  private[PipeVectorizerModel]
  class PipeVectorizerModelWriter(instance: PipeVectorizerModel) extends MLWriter {

    private case class Data(vocabulary: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.vocabulary)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }



  private class CountVectorizerModelReader extends MLReader[PipeVectorizerModel] {

    private val className = classOf[PipeVectorizerModel].getName

    override def load(path: String): PipeVectorizerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("vocabulary")
        .head()
      val vocabulary = data.getAs[Seq[String]](0).toArray
      val model = new PipeVectorizerModel(metadata.uid, vocabulary)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[PipeVectorizerModel] = new CountVectorizerModelReader

  @Since("1.6.0")
  override def load(path: String): PipeVectorizerModel = super.load(path)
}


@Since("1.6.0")
object PipeSliceVectorizerModel extends MLReadable[PipeSliceVectorizerModel] {

  private[PipeSliceVectorizerModel]
  class PipeSliceVectorizerModelWriter(instance: PipeSliceVectorizerModel) extends MLWriter {

    private case class Data(vocabulary: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.vocabulary)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }



  private class CountVectorizerModelReader extends MLReader[PipeSliceVectorizerModel] {

    private val className = classOf[PipeVectorizerModel].getName

    override def load(path: String): PipeSliceVectorizerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("vocabulary")
        .head()
      val vocabulary = data.getAs[Seq[String]](0).toArray
      val model = new PipeSliceVectorizerModel(metadata.uid, vocabulary)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[PipeSliceVectorizerModel] = new CountVectorizerModelReader

  @Since("1.6.0")
  override def load(path: String): PipeSliceVectorizerModel = super.load(path)
}
