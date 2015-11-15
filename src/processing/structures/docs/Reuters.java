package processing.structures.docs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 01.08.2015.
 */
public class Reuters extends Document {
    private String topic;
    private List<String> topicList;
    private int id;

    public Reuters() {
        super();
        topicList = new ArrayList<>();
        type = REUTORS;
    }

    public Reuters(String topic, String text) {
        super();
        this.topic = topic;
        this.text = text;

    }

    public void addTopic(String topic){
        topicList.add(topic);
    }

    public List<String> getTopicList() {
        return topicList;
    }


    public void buildId(){
        int result = 7;
        result = 31 * result + (text != null ? text.hashCode() : 0);
        id= result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getLabel() {
        return topic;
    }

    public void setLabel(String label) {
        this.topic = label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Reuters)) return false;
        if (!super.equals(o)) return false;

        Reuters reuters = (Reuters) o;

        return id == reuters.id;

    }

    @Override
    public int hashCode() {
        int result = id;
        return result;
    }
}
