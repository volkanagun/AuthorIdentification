package processing.structures.summary;

import java.io.Serializable;

/**
 * Created by wolf on 31.10.2015.
 */
public class PANDoc implements Serializable{
    private String docid;
    private String authorid;
    private Double label;

    private String text;


    public PANDoc(String docid, String authorid, Double label, String text) {
        this.docid = docid;
        this.authorid = authorid;
        this.label = label;
        this.text = text;
    }

    public String getAuthorid() {
        return authorid;
    }

    public void setAuthorid(String authorid) {
        this.authorid = authorid;
    }

    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    public Double getLabel() {
        return label;
    }

    public void setLabel(Double label) {
        this.label = label;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "PANDoc{" +
                "docid='" + docid + '\'' +
                ", label=" + label +
                '}';
    }
}
