package processing.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by wolf on 19.07.2015.
 */
public class RDDConfiguration implements Serializable{

    public static String MinNumberOfDocsPerAuthor = "cleaning.MinNumberOfDocsPerAuthor";
    public static String MaxNumberOfDocsPerAuthor = "cleaning.MaxNumberOfDocsPerAuthor";
    public static String MinNumberOfDocsPerBlogAuthor = "cleaning.MinNumberOfDocsPerBlogAuthor";
    public static String MinNumberOfDocsPerArticleAuthor = "cleaning.MinNumberOfDocsPerArticleAuthor";
    public static String MinNumberOfDocsPerTwitterAuthor = "cleaning.MinNumberOfDocsPerTwitterAuthor";
    public static String MaxNumberOfDocsPerBlogAuthor = "cleaning.MaxNumberOfDocsPerBlogAuthor";
    public static String MaxNumberOfDocsPerArticleAuthor = "cleaning.MaxNumberOfDocsPerArticleAuthor";
    public static String MaxNumberOfDocsPerTwitterAuthor = "cleaning.MaxNumberOfDocsPerTwitterAuthor";
    public static String MinimumDocCharLength = "cleaning.MinimumDocCharLength";
    public static String MinimumDocWordLength = "cleaning.MinimumDocWordLength";


    public static String EnableNGramChar = "char-ngrams.Enable";
    public static String EnableNGramWord = "word-ngrams.Enable";

    public static String MaxNGramCharLength = "char-ngrams.Max";
    public static String MinNGramCharLength = "char-ngrams.Min";

    public static String MaxNGramWordLength = "word-ngrams.Max";
    public static String MinNGramWordLength = "word-ngrams.Min";


    public static Properties properties = new Properties();

    public RDDConfiguration() {
        try {
            properties.load(new FileReader(new File("rdd.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Integer getIntegerProperty(String key){
        String value = properties.getProperty(key);
        return value!=null?Integer.parseInt(value):-1;
    }

    public String getStringProperty(String key){
        String value = properties.getProperty(key);
        return value!=null?value:"";
    }

    public Boolean getBooleanProperty(String key){
        String value = properties.getProperty(key);
        return value!=null?Boolean.parseBoolean(value):false;
    }


    public Integer getMinNumberOfDocsPerAuthor() {
        return getIntegerProperty(MinNumberOfDocsPerAuthor);
    }

    public Integer getMinNumberOfDocsPerBlogAuthor() {
        return getIntegerProperty(MinNumberOfDocsPerBlogAuthor);
    }

    public Integer getMinNumberOfDocsPerArticleAuthor() {
        return getIntegerProperty(MinNumberOfDocsPerArticleAuthor);
    }

    public Integer getMinNumberOfDocsPerTweeterAuthor() {
        return getIntegerProperty(MinNumberOfDocsPerTwitterAuthor);
    }

    public Integer getMaxNumberOfDocsPerAuthor() {
        return getIntegerProperty(MaxNumberOfDocsPerAuthor);
    }

    public Integer getMaxNumberOfDocsPerBlogAuthor() {
        return getIntegerProperty(MaxNumberOfDocsPerBlogAuthor);
    }

    public Integer getMaxNumberOfDocsPerArticleAuthor() {
        return getIntegerProperty(MaxNumberOfDocsPerArticleAuthor);
    }

    public Integer getMaxNumberOfDocsPerTweeterAuthor() {
        return getIntegerProperty(MaxNumberOfDocsPerTwitterAuthor);
    }

    public Integer getMinimumDocCharLength() {
        return getIntegerProperty(MinimumDocCharLength);
    }

    public Integer getMinimumDocWordLength() {
        return getIntegerProperty(MinimumDocWordLength);
    }



}
