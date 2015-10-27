package structures;

/**
 * Created by wolf on 27.10.2015.
 */
public class PAN extends Document{

    public PAN(String docid, String text) {
        super(docid, text, Document.PAN);
    }

    public PAN(String docid, String type, String text, String author) {
        super(docid, type, text, author);
    }

    public boolean unknownAuthor(){
        return author.equals(Document.UNKNOWN);
    }
}
