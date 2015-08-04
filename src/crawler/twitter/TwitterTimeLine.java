package crawler.twitter;

import operations.TextFile;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by wolf on 05.07.2015.
 */
public class TwitterTimeLine implements Serializable {

    private String filename = "twitter.properties";
    private Properties properties = new Properties();
    private Twitter twitter;
    private Integer pageStart = 1;

    public TwitterTimeLine() {
        load();
        loginNormal();
        //loginStream();
    }

    public Integer getPageStart() {
        return pageStart;
    }

    public TwitterTimeLine setPageStart(Integer pageStart) {
        this.pageStart = pageStart;
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


    public void download() {

        (new File("resources/twitter/")).mkdirs();

        while (true) {
            TextFile twitterFile = new TextFile("resources/twitter/twitter-1.xml");
            twitterFile.openNextBufferWrite();
            twitterFile.writeNextLine("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            twitterFile.writeNextLine("<ROOT LABEL=\"TWEET\">");


            for (int i = pageStart; i < 5; i++) {
                try {
                    Paging paging = new Paging(i + 1, 15);
                    List<Status> statusList = twitter.getHomeTimeline(paging);
                    for (Status status : statusList) {
                        User twitterUser = status.getUser();
                        String user = twitterUser.getName();
                        String screenName = twitterUser.getScreenName();
                        String message = status.getText().replaceAll("\n", " ");
                        String line = "<RESULT TYPE=\"CONTAINER\" LABEL=\"TWEET\">\n" +
                                "<RESULT TYPE=\"TEXT\" LABEL=\"AUTHORNAME\">\n"+user+"\n</RESULT>" +
                                "\n<RESULT TYPE=\"TEXT\" LABEL=\"TWEETTEXT\">\n"+message.trim()+"\n</RESULT>\n</RESULT>";

                        twitterFile.writeNextLine(line);
                    }

                    sleep(60000L);


                } catch (TwitterException e) {
                    e.printStackTrace();
                    Long wait = (long) e.getRateLimitStatus().getResetTimeInSeconds() * 1000;
                    sleep(wait);
                }
            }
            twitterFile.writeNextLine("</ROOT>");
            twitterFile.closeBufferWrite();
            sleep(100000L);
        }
    }

    public static void main(String[] args) {
        TwitterTimeLine twitterTimeLine = new TwitterTimeLine().setPageStart(1);
        twitterTimeLine.download();
    }


}
