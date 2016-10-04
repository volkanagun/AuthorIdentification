package org.apache.spark.ml.pipes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared.HasLabelCol
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.mllib.clustering.{BisectingKMeans, GaussianMixture}
import org.apache.spark.mllib.linalg.{Vector => LVEC}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Created by wolf on 12.07.2016.
  */
class PipesClustering(override val uid: String) extends Transformer with DefaultParamsWritable{
  //cluster documents before weighting is done but after
  //vectorization is done
  //generate cluster vector for sample (vector of cluster belongings)
  //cluster documents based on different set of features
  //generate k feature clusters

  //use k dimensional vector as a feature
  //use k dimesional vector for each sample to weight sample features
  //extract metadata and model
  // 1. number of samples for each cluster
  // 2. tokens/sentences/paragraphs size in cluster
  // 3. samples in each cluster

  /**
    * Given an instance vector, weight this vector/predict-soft by GMM
    * A feature in this vector should be weighted by using the soft predictions
    * So basically a vector belongs to each mixture by a degree,
    * Find the impact of each feature in this vector with weight of assigning
    * it to certain GMMM
    * 1. IMP_W1(X_I) * W1 + IMP_W2(X_I) * W2 + IMP_W3(X_I) * W3
    * 2.
    */

  val clusterFeatureCol = "cluster-features"

  val assembleColumns = new StringArrayParam(this,"assembleCols","Fetaure Column that will be gathered...")

  def fit(df:DataFrame): Unit ={
    val sDF = df.select()

  }

  def assemble(df:DataFrame):DataFrame = {
    val vectorAssembler = new VectorAssembler()
      .setInputCols($(assembleColumns))
      .setOutputCol(clusterFeatureCol)

    vectorAssembler.transform(df)
  }
  def cluster(df:DataFrame):Unit={

    val rdd:RDD[LVEC] = df.select(clusterFeatureCol)
      .map(row => {row.getAs[LVEC](0)})

    val gmix = new GaussianMixture()
      .setK(20).setMaxIterations(100)

    val gmixModel = gmix.run(rdd)




  }

  override def transform(dataset: DataFrame): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???


}