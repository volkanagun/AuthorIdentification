package structures.stats;

import structures.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 19.07.2015.
 */
public class DocStats implements Serializable {


    private Document document;
    private NGramStats nGramStats;


    private Double charLength;
    private Double wordLength;

    private Double paragraphCharLength;
    private Double paragraphWordLength;

    private Double minParagraphCharLength;
    private Double minParagraphWordLength;

    private Double maxParagraphCharLength;
    private Double maxParagraphWordLength;

    private Double titleCharLength;
    private Double titleWordLength;

    private Double numberofParagraphs;
    private Double numberofEmptyParagraphs;

    private Double averageWordCharLength = 0d;
    private Double maximumWordCharLength = 0d;
    private Double minimumWordCharLength = Double.MAX_VALUE;


    public DocStats() {
    }

    public DocStats(Document document) {
        this.document = document;
    }

    public Document getDocument() {
        return document;
    }

    public DocStats buildFromWords(String[] words){

        for(String word:words){
            word = word.trim();
            if(word.length()>0) {
                double wordLength = word.length();
                if (wordLength > maximumWordCharLength) {
                    maximumWordCharLength = wordLength;
                }

                if (wordLength < minimumWordCharLength) {
                    minimumWordCharLength = wordLength;
                }

                averageWordCharLength += wordLength;
            }
        }

        averageWordCharLength /= words.length;
        return this;
    }

    public DocStats buildFromDocument(Document document) {
        this.document = document;
        List<String> paragraphList = new ArrayList<>();
        if (document.isBlog()) {
            Blog blog = (Blog) document;
            titleCharLength = (double) blog.getTitle().length();
            titleWordLength = (double) blog.getTitle().split("\\s").length;
            charLength = (double) blog.getText().length();
            wordLength = (double) blog.getText().split("\\s").length;
            paragraphList = blog.getParagraphList();
            numberofParagraphs = (double) paragraphList.size();


        } else if (document.isArticle()) {
            Article article = (Article) document;
            titleCharLength = Utils.charLength(article.getTitle());
            titleWordLength = Utils.wordLength(article.getTitle());
            charLength = Utils.charLength(article.getText());
            wordLength = Utils.wordLength(article.getText());
            paragraphList = article.getParagraphList();
            numberofParagraphs = (double) paragraphList.size();


        } else if (document.isTweet()) {
            Tweet tweet = (Tweet) document;
            charLength = Utils.charLength(tweet.getText());
            wordLength = Utils.wordLength(tweet.getText());
            titleCharLength = 0d;
            titleWordLength = 0d;
            numberofEmptyParagraphs = 0d;
            numberofParagraphs = 0d;
        } else if(document.isArticle()){
            Reuters reuters = (Reuters) document;
            charLength = Utils.charLength(reuters.getText());
            wordLength = Utils.wordLength(reuters.getText());
            titleCharLength = 0d;
            titleWordLength = 0d;
            numberofEmptyParagraphs = 0d;
            numberofParagraphs = 0d;
        }

        buildFromWords(document.getText().split("\\s"));
        return buildFromParagraph(paragraphList);
    }

    private DocStats buildFromParagraph(List<String> paragraphList){
        double charLen=0d, wordLen=0d, maxChar=0d, minChar=0d, maxWord=0d, minWord=0d, emptyCount=0d;
        double sz =(double) paragraphList.size();
        for(String paragraph:paragraphList){
            paragraph = paragraph.replaceAll("[\t\n\\s]", "");
            double cLen = Utils.charLength(paragraph);
            double wLen = Utils.wordLength(paragraph);
            if(cLen == 0 || wLen==0)
                emptyCount+=1d;

            if(cLen>maxChar)
            {
                maxChar = charLen;
            }

            if(cLen<minChar){
                minChar = charLen;
            }

            if(wLen>maxWord){
                maxWord = wordLen;
            }

            if(wLen < minWord){
                minWord = wLen;
            }

            charLen+=cLen/sz;
            wordLen+=wLen/sz;

        }

        numberofParagraphs = (double) paragraphList.size();
        numberofEmptyParagraphs = emptyCount;
        maxParagraphCharLength = maxChar;
        maxParagraphWordLength = maxWord;
        minParagraphCharLength = minChar;
        minParagraphWordLength = minWord;
        paragraphCharLength = charLen;
        paragraphWordLength = wordLen;
        return this;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public NGramStats getnGramStats() {
        return nGramStats;
    }

    public void setnGramStats(NGramStats nGramStats) {
        this.nGramStats = nGramStats;
    }


    public Double getCharLength() {
        return charLength;
    }

    public void setCharLength(Double charLength) {
        this.charLength = charLength;
    }

    public Double getWordLength() {
        return wordLength;
    }

    public void setWordLength(Double wordLength) {
        this.wordLength = wordLength;
    }

    public Double getParagraphCharLength() {
        return paragraphCharLength;
    }

    public void setParagraphCharLength(Double paragraphCharLength) {
        this.paragraphCharLength = paragraphCharLength;
    }

    public Double getParagraphWordLength() {
        return paragraphWordLength;
    }

    public void setParagraphWordLength(Double paragraphWordLength) {
        this.paragraphWordLength = paragraphWordLength;
    }

    public Double getMinParagraphCharLength() {
        return minParagraphCharLength;
    }

    public void setMinParagraphCharLength(Double minParagraphCharLength) {
        this.minParagraphCharLength = minParagraphCharLength;
    }

    public Double getMinParagraphWordLength() {
        return minParagraphWordLength;
    }

    public void setMinParagraphWordLength(Double minParagraphWordLength) {
        this.minParagraphWordLength = minParagraphWordLength;
    }

    public Double getMaxParagraphCharLength() {
        return maxParagraphCharLength;
    }

    public void setMaxParagraphCharLength(Double maxParagraphCharLength) {
        this.maxParagraphCharLength = maxParagraphCharLength;
    }

    public Double getMaxParagraphWordLength() {
        return maxParagraphWordLength;
    }

    public void setMaxParagraphWordLength(Double maxParagraphWordLength) {
        this.maxParagraphWordLength = maxParagraphWordLength;
    }

    public Double getTitleCharLength() {
        return titleCharLength;
    }

    public void setTitleCharLength(Double titleCharLength) {
        this.titleCharLength = titleCharLength;
    }

    public Double getTitleWordLength() {
        return titleWordLength;
    }

    public void setTitleWordLength(Double titleWordLength) {
        this.titleWordLength = titleWordLength;
    }

    public Double getNumberofParagraphs() {
        return numberofParagraphs;
    }

    public void setNumberofParagraphs(Double numberofParagraphs) {
        this.numberofParagraphs = numberofParagraphs;
    }

    public Double getNumberofEmptyParagraphs() {
        return numberofEmptyParagraphs;
    }

    public void setNumberofEmptyParagraphs(Double numberofEmptyParagraphs) {
        this.numberofEmptyParagraphs = numberofEmptyParagraphs;
    }

    public Double getAverageWordCharLength() {
        return averageWordCharLength;
    }

    public void setAverageWordCharLength(Double averageWordCharLength) {
        this.averageWordCharLength = averageWordCharLength;
    }

    public Double getMaximumWordCharLength() {
        return maximumWordCharLength;
    }

    public void setMaximumWordCharLength(Double maximumWordCharLength) {
        this.maximumWordCharLength = maximumWordCharLength;
    }

    public Double getMinimumWordCharLength() {
        return minimumWordCharLength;
    }

    public void setMinimumWordCharLength(Double minimumWordCharLength) {
        this.minimumWordCharLength = minimumWordCharLength;
    }



}
