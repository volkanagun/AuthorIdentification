package structures;

/**
 * Created by wolf on 27.10.2015.
 */
public class PAN extends Document{

    public PAN(String docid, String text) {
        super(docid, text, Document.PAN);
    }

    public PAN(String docid, String author, String text) {
        super(docid, Document.PAN, text, author);
    }

    public PAN() {
        this.setType(Document.PAN);
    }

    public boolean unknownAuthorText(){
        return text!=null && author.equals(Document.UNKNOWN);
    }

    public boolean knownAuthorText(){
        return text!=null && !unknownAuthorText();
    }

    public boolean authorDocPair(){
        return text == null;
    }
}
