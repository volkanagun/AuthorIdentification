package structures.stats;

import org.apache.commons.collections.IteratorUtils;
import structures.Article;
import structures.Blog;
import structures.Document;

import java.io.Serializable;
import java.util.*;

/**
 * Created by wolf on 19.07.2015.
 */
public class AuthorStats implements Serializable {


    public static String BLOG = "BLOG";
    public static String ARTICLE = "ARTICLE";
    public static String TWEET = "TWEET";

    private List<Document> documentList;
    private Map<String, List<DocStats>> typeMap;
    private Map<String, List<DocStats>> genreMap;

    private BaseStats baseStats;
    private Map<String, BaseStats> typeSummaryMap;
    private Map<String, BaseStats> genreSummaryMap;

    private String authorname;
    private Double numberOfDocuments;

    public AuthorStats() {
    }

    public AuthorStats(String authorname) {
        this.authorname = authorname;
        this.typeMap = new HashMap<>();
        this.genreMap = new HashMap<>();
        this.typeSummaryMap = new HashMap<>();
        this.genreSummaryMap = new HashMap<>();

    }


    public List<DocStats> getByType(String typeKey){
        List<DocStats> docStats = typeMap.get(typeKey);
        if(docStats==null){
            docStats = new ArrayList<>();
            typeMap.put(typeKey, docStats);
            return docStats;
        }
        else{
            return docStats;
        }
    }



    public List<DocStats> getByGenre(String typeKey){
        List<DocStats> docStats = genreMap.get(typeKey);
        if(docStats==null){
            docStats = new ArrayList<>();
            genreMap.put(typeKey, docStats);
            return docStats;
        }
        else{
            return docStats;
        }
    }



    public Integer blogCount(){
        return getByType(BLOG).size();
    }
    public Integer articleCount(){
        return getByType(ARTICLE).size();
    }
    public Integer tweetCount(){
        return getByType(TWEET).size();
    }

    private void filterByMinCountRandom(Integer minCount, List<DocStats> documentList){

        int adjustSize = documentList.size() - minCount;
        for(int i=0; i<adjustSize; i++)
        {
            Random rnd = new Random(documentList.size());
            documentList.remove(rnd.nextInt());
        }
    }

    public void filterBlogsByMinCountRandom(Integer minCount){
        filterByMinCountRandom(minCount, getByType(BLOG));
    }

    public void filterArticlesByMinCountRandom(Integer minCount){
        filterByMinCountRandom(minCount,getByType(ARTICLE));
    }

    public void filterTweetsByMinCountRandom(Integer minCount){
        filterByMinCountRandom(minCount, getByType(TWEET));
    }


    public AuthorStats typeFromDocStats(Iterable<DocStats> documentList){


        Iterator<DocStats> iter = documentList.iterator();
        numberOfDocuments=0d;

        while(iter.hasNext()){
            DocStats docStat=iter.next();
            if(docStat.getDocument().isArticle()){
                getByType(ARTICLE).add(docStat);

            }
            else if(docStat.getDocument().isBlog()){
                getByType(BLOG).add(docStat);
            }
            else if(docStat.getDocument().isTweet()){
                getByType(TWEET).add(docStat);
            }

            numberOfDocuments++;
        }

        baseStats = new BaseStats().baseStatsFromDocStats(documentList);

        return this;
    }

    public AuthorStats genreFromDocStats(Iterable<DocStats> documentList){

        Iterator<DocStats> iter = documentList.iterator();
        while(iter.hasNext()){
            DocStats docStat = iter.next();
            Document doc= docStat.getDocument();
            if(doc.isBlog()){
                List<DocStats> docStatsList = getByGenre(((Blog)docStat.getDocument()).getGenre());
                docStatsList.add(docStat);
            }
            else if(doc.isArticle()){
                List<DocStats> docStatsList = getByGenre(((Article)docStat.getDocument()).getGenre());
                docStatsList.add(docStat);
            }
            else if(doc.isTweet()){
                List<DocStats> docStatsList = getByGenre(TWEET);
                docStatsList.add(docStat);
            }
        }

        baseStats = new BaseStats().baseStatsFromDocStats(documentList);
        return this;
    }



    public String getAuthorname() {
        return authorname;
    }

    public void setAuthorname(String authorname) {
        this.authorname = authorname;
    }


    public List<Document> getDocumentList() {
        return documentList;
    }

    public void setDocumentList(List<Document> documentList) {
        this.documentList = documentList;
    }

    public Map<String, List<DocStats>> getTypeMap() {
        return typeMap;
    }

    public void setTypeMap(Map<String, List<DocStats>> typeMap) {
        this.typeMap = typeMap;
    }

    public Map<String, List<DocStats>> getGenreMap() {
        return genreMap;
    }

    public void setGenreMap(Map<String, List<DocStats>> genreMap) {
        this.genreMap = genreMap;
    }

    public BaseStats getBaseStats() {
        return baseStats;
    }

    public void setBaseStats(BaseStats baseStats) {
        this.baseStats = baseStats;
    }

    public Map<String, BaseStats> getTypeSummaryMap() {
        return typeSummaryMap;
    }

    public void setTypeSummaryMap(Map<String, BaseStats> typeSummaryMap) {
        this.typeSummaryMap = typeSummaryMap;
    }

    public Map<String, BaseStats> getGenreSummaryMap() {
        return genreSummaryMap;
    }

    public void setGenreSummaryMap(Map<String, BaseStats> genreSummaryMap) {
        this.genreSummaryMap = genreSummaryMap;
    }

    public Double getNumberOfDocuments() {
        return numberOfDocuments;
    }

    public void setNumberOfDocuments(Double numberOfDocuments) {
        this.numberOfDocuments = numberOfDocuments;
    }

    @Override
    public String toString() {
        return "AuthorStats{" +
                "authorname='" + authorname + '\'' +
                ", baseStats=" + baseStats +
                '}';
    }
}
