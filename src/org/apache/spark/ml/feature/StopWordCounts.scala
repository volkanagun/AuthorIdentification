package org.apache.spark.ml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{functions, DataFrame}
import org.apache.spark.sql.types.{StructField, StringType, ArrayType, StructType}
import org.apache.spark.sql.functions._
import scala.io.Source

/**
 * Created by wolf on 31.10.2015.
 */
class StopWordCounts(override val uid:String) extends Transformer with HasInputCol with HasOutputCol{

  val filename = "resources/dictionary/stopwords/stopwords.txt"
  lazy val stopwords=load

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this()=this(Identifiable.randomUID("stopwords"))


  def load:List[String]={
    Source.fromFile(filename).mkString.split("\n").toList
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema);
    val metadata = outputSchema($(outputCol)).metadata

    val stops = functions.udf{
      sentences : Seq[Seq[String]] =>{
        
        var vector = Array.fill[Int](stopwords.size)(0)
        sentences.foreach(sentence=>{
          sentence.foreach(word=>{
            val index = stopwords.indexOf(word)
            if(index >= 0){
              val value:Int = vector.apply(index)+1
              vector.update(index, value)
            }
          })
        })
        vector
      }
    }
    
    dataset.select(col("*"), stops(col($(inputCol))).as($(outputCol), metadata))
    
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(ArrayType(StringType, false))), s"Input type must be Array[Array[String]] but got $inputType.")
    val outputFields = schema.fields :+ StructField($(outputCol),ArrayType(StringType,false), schema($(inputCol)).nullable)
    StructType(outputFields)
  }
}
