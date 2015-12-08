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

package org.apache.spark.ml.feature.extraction

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * :: Experimental ::
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 */
@Experimental
class ModifiedTFIDF(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("labelCounter"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Number of features.  Should be > 0.
   * (default = 2^18^)
   * @group param
   */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))
  setDefault(numFeatures -> (1 << 18))



  /** @group getParam */
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)


  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val hashingTF = new feature.HashingTF($(numFeatures))


    val t = udf {
      sentences:Seq[Seq[String]]=>{
        var terms = Array[String]()
        sentences.foreach(words=> terms = terms++words)
        hashingTF.transform(terms)

      }

    }
    val metadata = outputSchema($(outputCol)).metadata
    val dframe = dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
    dframe
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType[ArrayType[String]], but got $inputType.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): ModifiedTFIDF = defaultCopy(extra)
}
