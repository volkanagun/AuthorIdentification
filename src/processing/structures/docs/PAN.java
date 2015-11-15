package processing.structures.docs;

/**
 * Created by wolf on 27.10.2015.
 */
public class PAN extends Document{

    public PAN(String docid, String text) {
        super(docid, text, PAN);
    }

    public PAN(String docid, String author, String text) {
        super(docid, PAN, text, author);
    }

    public PAN() {
        this.setType(PAN);
    }

    public boolean unknownAuthorText(){
        return text!=null && (author==null || author.equals(UNKNOWN));
    }

    public boolean knownAuthorText(){
        return text!=null && author!=null && !author.equals(UNKNOWN);
    }

    public boolean authorDocPair(){
        return text == null && author!=null && !author.equals(UNKNOWN);
    }
}
