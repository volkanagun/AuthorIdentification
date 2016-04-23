package processing.structures.stats;

/**
 * Created by wolf on 07.01.2016.
 */
public class DatasetStats {
    private String type;
    private Long numberOfClasses;
    private Long averageInstancePerClass;
    private Long totalInstancePerClass;
    private Long minInstancePerClass;
    private Long maxInstancePerClass;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getNumberOfClasses() {
        return numberOfClasses;
    }

    public void setNumberOfClasses(Long numberOfClasses) {
        this.numberOfClasses = numberOfClasses;
    }

    public Long getAverageInstancePerClass() {
        return averageInstancePerClass;
    }

    public void setAverageInstancePerClass(Long averageInstancePerClass) {
        this.averageInstancePerClass = averageInstancePerClass;
    }

    public Long getTotalInstancePerClass() {
        return totalInstancePerClass;
    }

    public void setTotalInstancePerClass(Long totalInstancePerClass) {
        this.totalInstancePerClass = totalInstancePerClass;
    }

    public Long getMinInstancePerClass() {
        return minInstancePerClass;
    }

    public void setMinInstancePerClass(Long minInstancePerClass) {
        this.minInstancePerClass = minInstancePerClass;
    }

    public Long getMaxInstancePerClass() {
        return maxInstancePerClass;
    }

    public void setMaxInstancePerClass(Long maxInstancePerClass) {
        this.maxInstancePerClass = maxInstancePerClass;
    }
}
