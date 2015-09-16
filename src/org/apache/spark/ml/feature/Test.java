package org.apache.spark.ml.feature;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * Created by wolf on 16.09.2015.
 */
public class Test {

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setAppName("Feature Processing")
                .setMaster("local[6]")
                .set("spark.executor.memory", "20g")
                .set("spark.broadcast.blockSize", "50")
                /*.set("spark.logConf", "true")*/
                .set("log4j.rootCategory", Level.OFF.getName());
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        JavaRDD<Row> jrdd = context.parallelize(Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark. I wish Java could use case classes"),
                RowFactory.create(1, "Logistic regression models are neat.")
        ));

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });

        DataFrame textData = sqlContext.createDataFrame(jrdd,schema);

        SentenceDetector detector = new SentenceDetector().setInputCol("text").setOutputCol("sentences");
        DataFrame sentenceData = detector.transform(textData);
        for(Row row:sentenceData.select("sentences").take(3)){
            System.out.println(row);
        }

    }
}
