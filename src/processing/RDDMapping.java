package processing;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import structures.Document;
import structures.Reuters;
import structures.stats.AuthorStats;
import structures.stats.DocStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wolf on 19.07.2015.
 */
public class RDDMapping implements Serializable {
    private static RDDConfiguration configuration = new RDDConfiguration();

    public static class BuildDocStats implements FlatMapFunction<Tuple2<String, String>, DocStats> {
        @Override
        public Iterable<DocStats> call(Tuple2<String, String> v1) throws Exception {
            List<Document> documentList = XMLParser.parseDocument(v1._2());
            List<DocStats> docStatsList = new ArrayList<DocStats>();
            for(Document document:documentList){
                docStatsList.add(new DocStats().buildFromDocument(document));
            }
            return docStatsList;
        }
    }

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Filtering based on statistics">


    public static class AuthorDocCountFilter implements Function<AuthorStats, Boolean> {
        @Override
        public Boolean call(AuthorStats v1) throws Exception {
            return v1.getNumberOfDocuments() >= configuration.getMinNumberOfDocsPerAuthor();
        }
    }

    public static class DocWordCountFilter implements Function<DocStats, Boolean> {
        @Override
        public Boolean call(DocStats v1) throws Exception {
            return v1.getWordLength() >= configuration.getMinimumDocWordLength();
        }
    }

    public static class DocCharCountFilter implements Function<DocStats, Boolean> {
        @Override
        public Boolean call(DocStats v1) throws Exception {
            return v1.getCharLength() >= configuration.getMinimumDocCharLength();
        }
    }


    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////



    public static class GroupByPairAuthorStats implements Function<DocStats,String>{

        @Override
        public String call(DocStats v1) throws Exception {
            Document doc = v1.getDocument();
            return doc.getAuthor();
        }
    }


    public static class BuildAuthorStats implements Function<Tuple2<String, Iterable<DocStats>>, AuthorStats> {
        @Override
        public AuthorStats call(Tuple2<String, Iterable<DocStats>> v1) throws Exception {
            return new AuthorStats(v1._1()).typeFromDocStats(v1._2());
        }
    }

    public static class BuildGenreStats implements Function<Tuple2<String, Iterable<DocStats>>, AuthorStats> {
        @Override
        public AuthorStats call(Tuple2<String, Iterable<DocStats>> v1) throws Exception {
            return new AuthorStats(v1._1()).genreFromDocStats(v1._2());
        }
    }



}
