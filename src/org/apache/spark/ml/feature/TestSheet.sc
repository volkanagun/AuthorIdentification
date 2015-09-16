import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.SentenceDetector
import org.apache.spark.sql.SQLContext
object Test{
  val sparkContext = new SparkContext("local","Test")
  val sqlContext = new SQLContext(sparkContext);
  val sentenceData = sqlContext.createDataFrame(Seq(
    (0, "Hi I heard about Spark. I wish Java could use case classes"),
    (1, "Logistic regression models are neat.")
  )).toDF("label", "text")
  val detector = new SentenceDetector();
  println("Hello World")
}