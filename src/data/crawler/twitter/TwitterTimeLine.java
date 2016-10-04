package data.crawler.twitter;

import data.dataset.XMLParser;
import data.document.Document;
import data.dataset.XYZParser;
import options.Resources;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import util.TextFile;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.*;

/**
 * Created by wolf on 05.07.2015.
 */
public class TwitterTimeLine implements Serializable {

    private String filename = "conf/twitter.properties";
    private Properties properties = new Properties();
    private Twitter twitter;
    private Integer pageStart = 1, pageLength = 15, numPages = 100;
    private Integer sessionMax = 200;

    public TwitterTimeLine() {
        load();
        loginNormal();
        //loginStream();
    }

    public Integer getPageLength() {
        return pageLength;
    }

    public TwitterTimeLine setPageLength(Integer pageLength) {
        this.pageLength = pageLength;
        return this;
    }

    public Integer getPageStart() {
        return pageStart;
    }

    public TwitterTimeLine setPageStart(Integer pageStart) {
        this.pageStart = pageStart;
        return this;
    }

    public Integer getSessionMax() {
        return sessionMax;
    }

    public TwitterTimeLine setSessionMax(Integer sessionMax)  {
        this.sessionMax = sessionMax;
        return this;
    }

    public Integer getNumPages() {
        return numPages;
    }

    public TwitterTimeLine setNumPages(Integer numPages) {
        this.numPages = numPages;
        return this;
    }

    public void load() {

        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(filename);
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loginNormal() {
        String consumerKey = properties.getProperty("oauth.consumerKey");
        String consumerSecret = properties.getProperty("oauth.consumerSecret");
        String accessToken = properties.getProperty("oauth.accessToken");
        String accessTokenSecret = properties.getProperty("oauth.accessTokenSecret");

        if (consumerKey != null && consumerSecret != null && accessToken != null && accessTokenSecret != null) {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.setOAuthConsumerKey(consumerKey);
            builder.setOAuthConsumerSecret(consumerSecret);
            Configuration configuration = builder.build();
            TwitterFactory factory = new TwitterFactory(configuration);
            twitter = factory.getInstance(new AccessToken(accessToken, accessTokenSecret));
            //twitter.setOAuthConsumer(consumerKey, consumerSecret);
            //twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
        }
    }


    public void sleep(Long milisecs) {
        try {
            Thread.sleep(milisecs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void eliminateDuplicates(String outFilename){
        String mainDirectory = Resources.DocumentResources$.MODULE$.twitterDir();
        String outputFilename = mainDirectory+outFilename;
        List<File> fileList = TextFile.getFileList(mainDirectory);
        Set<Document> documentSet= new HashSet<>();
        Set<String> contentSet= new HashSet<>();

        String rootOpenXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ROOT LABEL=\"TWEET\">\n";
        String rootCloseXML = "</ROOT>";

        int count = 0;
        for(File file:fileList){
            Seq documents = XMLParser.parseDocument(file.getName(), TextFile.readText(file));
            List<Document> tweets = JavaConversions.seqAsJavaList(documents);
            for(Document tweet:tweets){
                if(!contentSet.contains(tweet.getText())){
                    documentSet.add(tweet);
                    contentSet.add(tweet.getText());
                    System.out.println("Unique tweet ....");
                }
                else{
                    System.out.println("Duplicate tweet count: "+count);
                    count++;
                }
            }
        }

        TextFile outFile = new TextFile(outputFilename);
        outFile.openBufferWrite();
        outFile.writeNextLine(rootOpenXML);

        for(Document tweet:documentSet){
            outFile.writeNextLine(tweet.toXML());
        }

        outFile.writeNextLine(rootCloseXML);
        outFile.closeBufferWrite();

    }

    protected String cleanMessage(String message){
        String result = message.replaceAll("<","&lt;");
        result = result.replaceAll(">","&gt;");
        result = result.replaceAll("&","&amp;");
        return result.trim();
    }

    public void download() {
        String mainDirectory = Resources.DocumentResources$.MODULE$.twitterDir();
        String mainFilename = mainDirectory + "twitter-1.xml";
        (new File(mainDirectory)).mkdirs();
        int sessionCount = 0;
        while (sessionCount < sessionMax) {
            TextFile twitterFile = new TextFile(mainFilename);
            twitterFile.openNextBufferWrite();
            twitterFile.writeNextLine("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            twitterFile.writeNextLine("<ROOT LABEL=\""+ Resources.DocumentModel$.MODULE$.TWITTERDOC()+"\">");

            for (int i = pageStart; i < numPages; i++) {
                try {

                    Paging paging = new Paging(i, pageLength);
                    List<Status> statusList = twitter.getHomeTimeline(paging);

                    for (int j = 0; j < statusList.size(); j++) {
                        Status status = statusList.get(j);
                        User twitterUser = status.getUser();
                        String user = twitterUser.getName();
                        String screenName = twitterUser.getScreenName();
                        String message = status.getText().replaceAll("\n", " ");
                        message = cleanMessage(message);
                        String line = "<RESULT TYPE=\"CONTAINER\" LABEL=\""+ Resources.DocumentModel$.MODULE$.TWEET()+"\">\n" +
                                "<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\n" + user + "\n</RESULT>" +
                                "\n<RESULT TYPE=\"TEXT\" LABEL=\"TWEETTEXT\">\n" + message.trim() + "\n</RESULT>\n</RESULT>";
                        System.out.println("Page: " + i + " Status: " + (j + 1));
                        twitterFile.writeNextLine(line);
                    }

                    System.out.println("Waiting for next page ("+i+"/"+numPages+")...");
                    sleep(60000L);


                } catch (TwitterException e) {
                    e.printStackTrace();
                    Long wait = (long) e.getRateLimitStatus().getResetTimeInSeconds() * 1000;
                    sleep(wait);
                }
            }
            twitterFile.writeNextLine("</ROOT>");
            twitterFile.closeBufferWrite();
            System.out.println("Waiting for next ("+sessionCount+"/"+sessionMax+")session...");
            sleep(100000L);
            sessionCount++;
        }
    }

    public static void main(String[] args) {
        TwitterTimeLine twitterTimeLine = new TwitterTimeLine()
                .setPageStart(1).setNumPages(100)
                .setPageLength(10).setSessionMax(1);

        twitterTimeLine.download();
        //twitterTimeLine.eliminateDuplicates("twitter-main-1.xml");
    }


}
