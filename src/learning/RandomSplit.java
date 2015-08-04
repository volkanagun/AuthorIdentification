package learning;

import operations.TextFile;
import org.apache.spark.examples.sql.JavaSparkSQL;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedExpressionCode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import scala.collection.Seq;
import util.PrintBuffer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wolf on 04.08.2015.
 */
public class RandomSplit implements Serializable {
    private String labelCol;
    private double[] weights;

    public RandomSplit(String labelCol, double[] weights) {
        this.labelCol = labelCol;
        this.weights = weights;
    }


    public DataFrame[] randomSplit(DataFrame df, PrintBuffer buffer) {
        //Obtain Distinct Labels
        Row[]  distinctLabels = df.select(labelCol).distinct().collect();
        Map<Double, DataFrame[]> distinctSplitMap = new HashMap<>();
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


        return new DataFrame[]{trainFrame, testFrame};
    }
}
