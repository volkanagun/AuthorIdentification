package structures.stats;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by wolf on 20.07.2015.
 */
public class BaseStats implements Serializable {

    private Double maximumDocCharLength;
    private Double maximumDocWordLength;
    private Double minimumDocCharLength;
    private Double minimumDocWordLength;
    private Double averageDocCharLength;
    private Double averageDocWordLength;

    private Double averageWordCharLength=0d;
    private Double maximumWordCharLength=0d;
    private Double minimumWordCharLength=Double.MAX_VALUE;


    public BaseStats baseStatsFromDocStats(Iterable<DocStats> documentIterator){

        double averageCharLength=0, averageWordLength=0,maximumCharLength=0, minimumCharLength=Double.MAX_VALUE, maximumWordLength=0, minimumWordLength=Double.MAX_VALUE;
        long count = 0;
        Iterator<DocStats> iter = documentIterator.iterator();
        while(iter.hasNext()){
            DocStats stats = iter.next();

            if(stats.getCharLength()<minimumCharLength){
                minimumCharLength = stats.getCharLength();
            }

            if(stats.getWordLength() < minimumWordLength){
                minimumWordLength = stats.getWordLength();
            }

            if(stats.getCharLength() > maximumCharLength){
                maximumCharLength = stats.getCharLength();
            }

            if(stats.getWordLength() > maximumWordLength){
                maximumWordLength = stats.getWordLength();
            }

            if(stats.getMaximumWordCharLength()>maximumWordCharLength){
                maximumWordCharLength = stats.getMaximumWordCharLength();
            }

            if(stats.getMinimumWordCharLength()<minimumWordCharLength){
                minimumWordCharLength = stats.getMinimumWordCharLength();
            }

            averageCharLength+=stats.getCharLength();
            averageWordLength+=stats.getWordLength();
            averageWordCharLength+=stats.getAverageWordCharLength();

            count++;
        }

        averageCharLength = averageCharLength/count;
        averageWordLength = averageWordLength/count;
        averageWordCharLength = averageWordCharLength/count;

        averageDocCharLength = averageCharLength;
        averageDocWordLength = averageWordLength;
        maximumDocCharLength = maximumCharLength;
        minimumDocCharLength = minimumCharLength;
        maximumDocWordLength = maximumWordLength;
        minimumDocWordLength = minimumWordLength;


        return this;
    }


    public Double getAverageDocCharLength() {
        return averageDocCharLength;
    }

    public void setAverageDocCharLength(Double averageDocCharLength) {
        this.averageDocCharLength = averageDocCharLength;
    }

    public Double getAverageDocWordLength() {
        return averageDocWordLength;
    }

    public void setAverageDocWordLength(Double averageDocWordLength) {
        this.averageDocWordLength = averageDocWordLength;
    }

    public Double getMaximumDocCharLength() {
        return maximumDocCharLength;
    }

    public void setMaximumDocCharLength(Double maximumDocCharLength) {
        this.maximumDocCharLength = maximumDocCharLength;
    }

    public Double getMaximumDocWordLength() {
        return maximumDocWordLength;
    }

    public void setMaximumDocWordLength(Double maximumDocWordLength) {
        this.maximumDocWordLength = maximumDocWordLength;
    }

    public Double getMinimumDocCharLength() {
        return minimumDocCharLength;
    }

    public void setMinimumDocCharLength(Double minimumDocCharLength) {
        this.minimumDocCharLength = minimumDocCharLength;
    }

    public Double getMinimumDocWordLength() {
        return minimumDocWordLength;
    }

    public void setMinimumDocWordLength(Double minimumDocWordLength) {
        this.minimumDocWordLength = minimumDocWordLength;
    }

    public Double getAverageWordCharLength() {
        return averageWordCharLength;
    }

    public void setAverageWordCharLength(Double averageWordCharLength) {
        this.averageWordCharLength = averageWordCharLength;
    }

    public Double getMaximumWordCharLength() {
        return maximumWordCharLength;
    }

    public void setMaximumWordCharLength(Double maximumWordCharLength) {
        this.maximumWordCharLength = maximumWordCharLength;
    }

    public Double getMinimumWordCharLength() {
        return minimumWordCharLength;
    }

    public void setMinimumWordCharLength(Double minimumWordCharLength) {
        this.minimumWordCharLength = minimumWordCharLength;
    }

    @Override
    public String toString() {
        return "BaseStats{" +
                "maximumDocCharLength=" + maximumDocCharLength +
                ", maximumDocWordLength=" + maximumDocWordLength +
                ", minimumDocCharLength=" + minimumDocCharLength +
                ", minimumDocWordLength=" + minimumDocWordLength +
                ", averageDocCharLength=" + averageDocCharLength +
                ", averageDocWordLength=" + averageDocWordLength +
                ", averageWordCharLength=" + averageWordCharLength +
                ", maximumWordCharLength=" + maximumWordCharLength +
                ", minimumWordCharLength=" + minimumWordCharLength +
                '}';
    }
}
