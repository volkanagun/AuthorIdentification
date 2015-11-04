package org.apache.spark.ml.feature

import org.apache.hadoop.record.meta.VectorTypeID
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamValidators, IntParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source

/**
 * Created by wolf on 31.10.2015.
 */
class MessageLength(override val uid:String) extends Transformer with HasInputCol with HasOutputCol{



  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  setDefault(numFeatures -> (1<<18))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this()=this(Identifiable.randomUID("stopwords"))


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata

    val wordcounts = functions.udf{
      sentences : Seq[Seq[String]] =>{

        var sentenceTotalLength = 0
        var wordTotalLength = 0
        var puncTotalLength = 0
        var digitTotalLength = 0
        var sentenceCount = 0
        var wordCount = 0
        var digitCount = 0
        var puncCount =0
        var tokenCount=0
        sentences.foreach(sentence=>{

          sentence.foreach(token=>{
            if(token.matches("\\p{L}+")){
              wordCount+=1
              wordTotalLength+=token.length
            }
            else if(token.matches("\\p{Punc}+")){
              puncCount+=1
              puncTotalLength+=token.length
            }
            else if(token.matches("\\d+")){
              digitCount+=1
              digitTotalLength+= token.length
            }

            tokenCount+=1
            sentenceTotalLength+=token.length

          })

          sentenceCount+=1
        })

        //A more general combination approach must be taken here...
        //Consider each regex as a dictionary below some combinations represented
        //ToDo make them all automatic when dictionaries are given
        Vector[Double](
        sentenceTotalLength,wordTotalLength,
        puncTotalLength,
        digitTotalLength,
        sentenceTotalLength/tokenCount,
        sentenceTotalLength/wordTotalLength,
        sentenceTotalLength/puncTotalLength,
        sentenceTotalLength/digitTotalLength,
        sentenceTotalLength/sentenceCount,
        wordCount/tokenCount,puncCount/tokenCount,digitCount/tokenCount,
        puncTotalLength/puncCount, wordTotalLength/wordCount, digitTotalLength/digitCount
        )
      }
    }

    dataset.select(col("*"), wordcounts(col($(inputCol))).as($(outputCol), metadata))
    
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType, false))), s"Input type must be Array[Array[String]] but got $inputType.")

    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}
