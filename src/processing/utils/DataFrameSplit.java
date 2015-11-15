package processing.utils;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import util.PrintBuffer;

import java.io.Serializable;

/**
 * Created by wolf on 04.08.2015.
 */
public class DataFrameSplit implements Serializable {
    private String labelCol;
    private double[] weights;

    public DataFrameSplit(String labelCol, double[] weights) {
        this.labelCol = labelCol;
        this.weights = weights;
    }


    public DataFrame[] randomSplit(DataFrame df, PrintBuffer buffer) {
        //Obtain Distinct Labels
        Row[]  distinctLabels = df.select(labelCol).distinct().collect();
        //Filter according to each label
        //Split According to each label
        Row row = distinctLabels[0];
        Double label = row.getDouble(0);
        DataFrame[] splitted = df.filter(df.col(labelCol).eqNullSafe(label)).randomSplit(weights);
        DataFrame trainFrame = splitted[0];
        DataFrame testFrame = splitted[1];

        buffer.addLine("Testing label counts...");

        for(int i=1; i<distinctLabels.length; i++){
            row = distinctLabels[i];
            label = row.getDouble(0);
            splitted = df.filter(df.col(labelCol).eqNullSafe(label)).randomSplit(weights);
            buffer.addLine("Label:"+label+"->"+splitted[1].count());
            trainFrame = trainFrame.unionAll(splitted[0]);
            testFrame = testFrame.unionAll(splitted[1]);
        }

        buffer.addLine("");
        return new DataFrame[]{trainFrame, testFrame};
    }
}
