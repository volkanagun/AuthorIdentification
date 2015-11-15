package processing.structures.docs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 08.07.2015.
 */
public class Article extends Document {
    private String genre;
    private String title;
    private List<String> paragraphList;

    public Article() {
        super();
        this.type = Document.ARTICLE;
        this.paragraphList = new ArrayList<>();
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public void addParagraph(String paragraph) {
        this.paragraphList.add(paragraph);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
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

    public List<String> getParagraphList() {
        return paragraphList;
    }

    public void setParagraphList(List<String> paragraphList) {
        this.paragraphList = paragraphList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Article a = (Article) o;
        return a.type.equals(type) && a.author.equals(author) && a.title.equals(title);
    }
}
