package structures.summary;

import java.util.List;

/**
 * Created by wolf on 31.10.2015.
 */
public class PANDoc {
    private String docid;
    private double label;
    private String text;


    public PANDoc(String docid,double label, String text) {
        this.docid = docid;
        this.label = label;
        this.text = text;
    }

    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    public double getLabel() {
        return label;
    }

    public void setLabel(double label) {
        this.label = label;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
