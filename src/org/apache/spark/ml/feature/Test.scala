import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, SentenceDetector}

import org.apache.spark.sql.SQLContext
object Test{
  val sparkContext = new SparkContext("local","Test")
  val sqlContext = new SQLContext(sparkContext);
  val sentenceData = sqlContext.createDataFrame(Seq(
    (0, "Hi I heard about Spark. I wish Java could use case classes"),
    (1, "Logistic regression models are neat.")
  )).toDF("label", "text")
  val detector = new SentenceDetector();
  detector.setInputCol("text")
  detector.setOutputCol("sentences")

  detector.transform(sentenceData).select("sentences").show()

  new HashingTF
}