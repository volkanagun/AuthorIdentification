package structures;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 08.07.2015.
 */
public class Blog extends Document {
    private List<String> paragraphList;
    private String genre;
    private String title;

    public Blog() {
        super();
        this.paragraphList = new ArrayList<>();
        this.type = Document.BLOG;
    }

    public Blog(String author, String text) {
        super(author, text);
        this.paragraphList = new ArrayList<>();
        this.type = Document.BLOG;
    }

    public void addParagraph(String paragraph) {
        this.paragraphList.add(paragraph);

    }

    public List<String> getParagraphList() {
        return paragraphList;
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

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Blog b = (Blog) o;
        return b.type.equals(type) && b.author.equals(author) && b.title.equals(title);
    }

}
