package processing;

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


    public static String MaxNGramCharLength = "ngrams.MaxNGramCharLength";
    public static String MinNGramCharLength = "ngrams.MinNGramCharLength";
    public static String MaxNGramWordLength = "ngrams.MaxNGramWordLength";
    public static String MinNGramWordLength = "ngrams.MinNGramWordLength";


    public static Properties properties = new Properties();

    public RDDConfiguration() {
        try {
            properties.load(new FileReader(new File("rdd.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Integer getMinNumberOfDocsPerAuthor() {
        String str = properties.getProperty(MinNumberOfDocsPerAuthor);
        if(str==null)
            return 10;
        else
            return Integer.parseInt(str);
    }

    public Integer getMinNumberOfDocsPerBlogAuthor() {

        return Integer.parseInt(properties.getProperty(MinNumberOfDocsPerBlogAuthor));
    }

    public Integer getMinNumberOfDocsPerArticleAuthor() {
        return Integer.parseInt(properties.getProperty(MinNumberOfDocsPerArticleAuthor));
    }

    public Integer getMinNumberOfDocsPerTweeterAuthor() {
        return Integer.parseInt(properties.getProperty(MinNumberOfDocsPerTwitterAuthor));
    }

    public Integer getMaxNumberOfDocsPerAuthor() {
        return Integer.parseInt(properties.getProperty(MaxNumberOfDocsPerAuthor));
    }

    public Integer getMaxNumberOfDocsPerBlogAuthor() {
        return Integer.parseInt(properties.getProperty(MaxNumberOfDocsPerBlogAuthor));
    }

    public Integer getMaxNumberOfDocsPerArticleAuthor() {
        return Integer.parseInt(properties.getProperty(MaxNumberOfDocsPerArticleAuthor));
    }

    public Integer getMaxNumberOfDocsPerTweeterAuthor() {
        return Integer.parseInt(properties.getProperty(MaxNumberOfDocsPerTwitterAuthor));
    }

    public Integer getMinimumDocCharLength() {
        String str = properties.getProperty(MinimumDocCharLength);
        if(str==null)
            return 30;
        else
            return Integer.parseInt(str);
    }

    public Integer getMinimumDocWordLength() {
        String str = properties.getProperty(MinimumDocWordLength);
        if(str==null)
            return 3;
        else
            return Integer.parseInt(str);
    }



}
