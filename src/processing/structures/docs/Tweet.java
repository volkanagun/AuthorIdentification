package processing.structures.docs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 08.07.2015.
 */
public class Tweet extends Document {
    private List<String> rtList;
    private List<String> links;

    public Tweet() {
        super();
        this.rtList = new ArrayList<>();
        this.links = new ArrayList<>();
        this.type = TWEET;
        super.setType(TWEET);
    }

    public Tweet(String author, String text) {
        super(author, text);
        this.rtList = new ArrayList<>();
        this.links = new ArrayList<>();
        this.type = TWEET;
        super.setType(TWEET);
    }

    @Override
    public String getAuthor() {
        return super.getAuthor();
    }

    @Override
    public void setAuthor(String author) {
        super.setAuthor(author);
    }

    @Override
    public String getText() {
        return super.getText();
    }

    @Override
    public void setText(String text) {
        super.setText(text);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tweet a = (Tweet) o;
        return a.type.equals(type) && a.author.equals(author) && a.text.equals(text);
    }
}
