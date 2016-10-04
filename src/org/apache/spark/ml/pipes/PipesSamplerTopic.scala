package org.apache.spark.ml.pipes

import java.io._

import language.topicality.TopicModeller
import options.Resources.LDAResources
import options.{Parameters, ParamsMorphology, Resources}
import org.apache.commons.io.FileUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.feature.{VectorAssembler, VectorSlicer}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.{Estimator, Model, TokenStem, Transformer}
import org.apache.spark.ml.param.{BooleanParam, IntParam, Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils, SchemaUtils}
import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

/**
  * Created by wolf on 08.09.2016.
  */
class SamplerHLDA(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("topicSampler"))

  val stemmer = ParamsMorphology.analyzer
  val sampleCol = new Param[String](this, "sampleColumn", "sample from this text column")
  val docIDCol = new Param[String](this, "docIDColumn", "sample document id")

  def setSampleCol(value: String) = set(sampleCol, value)

  def setDocIDCol(value: String) = set(docIDCol, value)


  override def transform(dataset: DataFrame): DataFrame = {

    val documents = dataset.select($(docIDCol), $(sampleCol)).map(row => {
      val docid = row.getAs[String](0)
      val tokens = row.getAs[Seq[String]](1)
      val words = tokens.filter(token => {
        token.matches("\\p{L}+")
      })
        .map(token => token.toLowerCase(Parameters.crrLocale))
      val stems = words.map(word => TokenStem.stemmingSnowball(stemmer, word))
      (docid, stems)
    }).collect()

    TopicModeller.createModel(documents)
    dataset
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}


class SamplerLDA(override val uid: String) extends Estimator[SamplerLDAModel] with HasInputCol with HasOutputCol {

  val k = new IntParam(this, "lda-k", "lda number of topics", (a: Int) => a > 0)
  val sliceNum = new IntParam(this, "slice-num", "number of slices for topic models")
  val forceGenerate = new BooleanParam(this, "create-lda", "force to create lda model")
  val modelFilename = new Param[String](this, "ldamodel", "lda model and dictionary")
  val vocabFilename = new Param[String](this, "ldavocabulary", "vocabulary used in lda model creation")
  val processCol = new Param[String](this, "modelprocesscolumn", "model process column for example slice column")


  def setK(value: Int): this.type = set(k, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setVocabularyCol(value: String): this.type = set(inputCol, value)

  def setProcessCol(value: String): this.type = set(processCol, value)


  def setSliceNum(value: Int): this.type = set(sliceNum, value)

  def setCreateModel(value: Boolean): this.type = set(forceGenerate, value)

  def setModelFilename(value: String): this.type = set(modelFilename, value)

  setDefault(k -> 20, forceGenerate -> true, modelFilename -> LDAResources.ldaModelDir,
    vocabFilename -> LDAResources.ldaModelVocabularyDir)


  def this() = this(Identifiable.randomUID("sampler-lda"))

  protected def exists(filename: String): Boolean = {
    new File(filename).exists()
  }

  protected def generateVocabulary(schema: StructType): Array[String] = {
    val inputAttr = AttributeGroup.fromStructField(schema($(inputCol)))
    inputAttr.attributes.get.map(attr => {
      attr.name.get
    })
  }

  protected def saveVocabulary(filename: String, vocab: Array[String]): this.type = {
    val oos = new ObjectOutputStream(new FileOutputStream(filename))
    oos.writeObject(vocab)
    oos.close
    this
  }


  protected def loadVocabulary(filename: String): Array[String] = {
    val oos = new ObjectInputStream(new FileInputStream(filename))
    val obj = oos.readObject()
    oos.close
    obj.asInstanceOf[Array[String]]
  }


  protected def loadVocabulary(schema: StructType, filename: String): Array[String] = {
    if (!$(forceGenerate) && exists(filename)) {
      loadVocabulary(filename)
    }
    else {
      val vocab = generateVocabulary(schema)
      saveVocabulary(filename, vocab)
      vocab
    }
  }


  override def fit(dataset: DataFrame): SamplerLDAModel = {
    //if a good lda model exists do not fit it just load it
    //filtered term counts

    val ldaFilename = LDAResources.ldaModelDir
    val vocabFilename = LDAResources.ldaModelVocabularyDir
    val force = $(forceGenerate)
    val model = if (!force && exists(ldaFilename) && exists(vocabFilename)) {
      val ldaModel = LocalLDAModel.load(ldaFilename)
      ldaModel
        .setFeaturesCol($(inputCol))
        .set(ldaModel.topicDistributionCol,$(outputCol))
    }
    else {
      val ldaModel = new LDA()
        .setFeaturesCol($(inputCol))
        .setTopicDistributionCol($(outputCol))
        .setK($(k)).fit(dataset)

      FileUtils.deleteDirectory(new File(ldaFilename))
      ldaModel.save(ldaFilename)
      ldaModel
    }

    val vocab = loadVocabulary(dataset.schema, vocabFilename)

    new SamplerLDAModel(model, vocab)
      .setInputCol($(processCol)).setOutputCol($(outputCol))
      .setSliceNum($(sliceNum))
  }

  override def copy(extra: ParamMap): Estimator[SamplerLDAModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {

    if($(forceGenerate)) {
      val inputColName = $(inputCol)
      val inputDataType = schema(inputColName)
      require(inputDataType.dataType == new VectorUDT(), s"The input column $inputColName must be vector type " +
        s"but got $inputDataType.")
    }

    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

class SamplerLDAModel(override val uid: String, val model: LDAModel, val vocab: Array[String]) extends Model[SamplerLDAModel] with HasInputCol with HasOutputCol {

  def this(model: LDAModel, vocab: Array[String]) = this(Identifiable.randomUID("samplerlda-model"), model, vocab)

  val sliceNum = new IntParam(this, "slice-num", "number of slices")


  def setSliceNum(value: Int): this.type = set(sliceNum, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)


  /**
    * Slices of columns or full text
    *
    * @param value
    * @return
    */
  def setInputCol(value: String): this.type = set(inputCol, value)


  override def copy(extra: ParamMap): SamplerLDAModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {

    //use a vectorizer for only some tokens as pipeline
    //divided text into chunks (paragraphs or sentences)
    //use vocabulary to vectorize paragraphs
    //apply topicality to each paragraph and find a sequence

    //use special vectorizer
    val tmpInName = "sliced-vectors"
    val tmpOutName = "topic-vectors"
    val outCol = $(outputCol)
    val numSlice = $(sliceNum)
    val vectorizer = new PipeSliceVectorizerModel(vocab)
      .setInputCol($(inputCol))
      .setOutputCol(tmpInName)
      .setSliceNum(numSlice)

    val slicedDF = vectorizer.transform(dataset)

    val modifiedDF = Range(0, numSlice).foldLeft[DataFrame](slicedDF) {
      case (df, index) => {
        val colInName = tmpInName + "_" + index
        val colOutName = tmpOutName + "$" + index

        println("Building topic model slice for ouput column: " + colOutName)

        val ddf = model.setFeaturesCol(colInName)
          .set(model.topicDistributionCol, colOutName)
          .transform(df)


        ddf
        //ddf.withColumnRenamed(outCol, colOutName)
      }
    }

    //modifiedDF.printSchema()


    val mergeCols = Range(0, numSlice).map(index => tmpOutName + "$" + index).toArray
    new VectorAssembler()
      .setInputCols(mergeCols)
      .setOutputCol(outCol)
      .transform(modifiedDF)


  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType

    require(inputDataType == ArrayType(ArrayType(StringType)), s"The input column $inputColName must be Array[Array[String]] type " +
      s"but got $inputDataType.")



    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}