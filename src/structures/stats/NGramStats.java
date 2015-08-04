package structures.stats;

import structures.Document;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by wolf on 19.07.2015.
 */
public class NGramStats implements Serializable {


    public static String TITLECHAR="TCHAR";
    public static String PARAGRAPHCHAR="PCHAR";
    public static String DOCUMENTCHAR="DCHAR";

    private Document document;
    private Map<String,Double> ngrams;

    public NGramStats() {
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public Map<String, Double> getNgrams() {
        return ngrams;
    }

    public void setNgrams(Map<String, Double> ngrams) {
        this.ngrams = ngrams;
    }

    //Wordgrams, chargrams, syntactic-grams

}
