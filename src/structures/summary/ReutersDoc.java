package structures.summary;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wolf on 01.08.2015.
 */
public class ReutersDoc implements Serializable {
    private String text;
    private List<String> sentences;
    private String topic;
    private int id;
    private double label;

    public ReutersDoc(String text, String topic) {
        this.text = text;
        this.topic = topic;
        this.id = text.hashCode();
    }

    public List<String> getSentences() {
        return sentences;
    }

    public void setSentences(List<String> sentences) {
        this.sentences = sentences;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }


    public double label() {
        return label;
    }

    public void label(double label){
        this.label = label;
    }

    public double getLabel() {
        return label;
    }

    public void setLabel(double label) {
        this.label = label;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReutersDoc)) return false;

        ReutersDoc that = (ReutersDoc) o;

        return getText().equals(that.getText());

    }

    @Override
    public int hashCode() {
        return getText().hashCode();
    }
}
