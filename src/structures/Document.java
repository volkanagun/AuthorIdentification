package structures;

import java.io.Serializable;

/**
 * Created by wolf on 08.07.2015.
 */
public class Document implements Serializable {
    public static String TWEET = "T";
    public static String BLOG = "B";
    public static String REUTORS = "R";
    public static String PAN = "P";
    public static String ARTICLE = "A";
    public static String UNKNOWN = "UNKNOWN";

    protected String type;
    protected String author;
    protected String text;
    protected String docid;

    public Document() {
    }

    public Document(String author, String text) {
        this.author = author;
        this.text = text;
    }

    public Document(String docid, String type,String text) {
        this.docid = docid;
        this.text = text;
        this.type = type;
        this.author = UNKNOWN;
    }

    public Document(String docid, String type,String text,  String author) {
        this.docid = docid;
        this.text = text;
        this.type = type;
        this.author = author;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public boolean isEmpty(){
        return author==null || text==null || author.isEmpty() || text.isEmpty();
    }

    public boolean isBlog(){
        return type!=null && type.equals(BLOG);
    }

    public boolean isArticle(){
        return type!=null && type.equals(ARTICLE);
    }

    public boolean isReuters(){
        return type!=null && type.equals(REUTORS);
    }

    public boolean isBlogArticle(){
        return type!=null && (type.equals(ARTICLE) || type.equals(BLOG));
    }

    public boolean isTweet(){
        return type!=null && type.equals(TWEET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Document document = (Document) o;

        if (type != null ? !type.equals(document.type) : document.type != null) return false;
        return !(author != null ? !author.equals(document.author) : document.author != null);

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (author != null ? author.hashCode() : 0);
        return result;
    }
}
