package learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.LabelCounter;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import processing.RDDProcessing;
import structures.summary.ReutersDoc;
import util.PrintBuffer;

import java.io.Serializable;

/**
 * Created by wolf on 01.08.2015.
 */
public class ReutersPipeLine implements Serializable{

    public void pipeline(JavaSparkContext sc, RDDProcessing processing){

        PrintBuffer buffer = new PrintBuffer();
        SQLContext sqlContext = new SQLContext(sc);
        RandomSplit rndSplit = new RandomSplit("label",new double[]{0.7,0.3});
        JavaRDD<ReutersDoc> reutersRDD = processing.reutersRDD(sc,buffer, 10);
        DataFrame df = sqlContext.applySchema(reutersRDD, ReutersDoc.class);
        DataFrame[] trainTestDf = rndSplit.randomSplit(df, buffer);
        DataFrame trainFrame = trainTestDf[0].cache();
        DataFrame testFrame = trainTestDf[1].cache();

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");

        LabelCounter hashingTF = new LabelCounter()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");


        DecisionTreeClassifier dc = new DecisionTreeClassifier();
        dc.setLabelCol("label");


        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, dc});


        CrossValidator crossval = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new MulticlassClassificationEvaluator());

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(hashingTF.numFeatures(), new int[]{100, 1000, 10000})
                //.addGrid(lr., new double[]{0.1, 0.01})
                .build();

        crossval.setEstimatorParamMaps(paramGrid);
        crossval.setNumFolds(10);


        CrossValidatorModel cvModel = crossval.fit(trainFrame);
        DataFrame predictions = cvModel.transform(testFrame);


        DataFrame frame =  predictions.select("prediction","label").toSchemaRDD();
        MulticlassMetrics metrics = new MulticlassMetrics(frame);
        long trainCount = trainFrame.count();
        long testCount = testFrame.count();
        double fmeasure = metrics.fMeasure();
        Matrix matrix = metrics.confusionMatrix();

        buffer.addLine("BOW Model with Decision Tree Classifier");
        buffer.addLine("Number of training instances: " + trainCount);
        buffer.addLine("Number of testing instances: " + testCount);
        buffer.addLine("F1-Measure:" + fmeasure);
        buffer.addLine("Confusion:" + matrix);

        //buffer.print();

        for(double i=0; i<10; i++){

            double fprate = metrics.falsePositiveRate(i);
            double precision = metrics.precision(i);
            double recall = metrics.recall(i);

            buffer.addLine("Label : "+i+"->");
            buffer.addLine("\tFalse Positives->"+fprate);
            buffer.addLine("\tPrecision Rate->"+precision);
            buffer.addLine("\tRecall Rate->"+recall);

        }

        sc.close();


        buffer.print();

       /* int count = 0; double trueCount=0;
        for (Row r: predictions.select("prediction", "label", "topic").collect()) {
            if(r.get(0).equals(r.get(1))) trueCount++;
            count++;
            //System.out.println("(" + r.get(0) + ", " + r.get(1) + ")");
        }*/

        //System.out.println("Accuracy : " + trueCount + "/" + count);

    }

    public static void main(String[] args){
        RDDProcessing processing = new RDDProcessing();
        JavaSparkContext sc = new JavaSparkContext(RDDProcessing.initLocal());
        ReutersPipeLine pipeLine = new ReutersPipeLine();
        pipeLine.pipeline(sc, processing);
    }
}
