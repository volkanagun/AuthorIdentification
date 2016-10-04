package org.apache.spark.ml.pipes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Created by wolf on 24.04.2016.
  */
class PipesMorphology(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  //Use sequence of predicted morphs for analysis different tags
  //Percentage of different tag sets for each document



  def this() = this(Identifiable.randomUID("morphology-features"))



  //Input sequence of sentences
  //Output postags
  override def transform(dataset: DataFrame): DataFrame = {
    //Take input column
    dataset
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}


