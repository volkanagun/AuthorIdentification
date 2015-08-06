package processing;


import crawler.web.LookupOptions;
import crawler.web.WebCleaner;
import org.mortbay.jetty.webapp.WebAppClassLoader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import structures.Article;
import structures.Blog;
import structures.Reuters;
import structures.Tweet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by wolf on 09.07.2015.
 */
public class XMLParser implements Serializable {

    private static String clean(String text){
        text = text.replaceAll("(?!(<RESULT.*?>|<ROOT.*?>|</RESULT>|</ROOT>))<.*?>","");
        return text;
    }
    private static Locale trLocale = new Locale("tr");

    private static InputStream inputStream(String text) {
        text = clean(text);
        return new ByteArrayInputStream(text.getBytes(Charset.forName("UTF-8")));
    }

    public static List<structures.Reuters> parseReuters(String filename, String text) {
        //Get the DOM Builder Factory

        List<structures.Reuters> anyDocumentList = new ArrayList<>();
        InputStream stream = inputStream(text);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = null;
        try {
            builder = factory.newDocumentBuilder();
            Document document = builder.parse(stream);
            NodeList nodeList = document.getElementsByTagName("ROOT");

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node item = nodeList.item(i);
                Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
                String attributeValue = itemAttribute.getNodeValue();

                switch (attributeValue) {
                    case "R":{
                        List<Reuters> anyDocument = parseReuters(item);
                        anyDocumentList.addAll(anyDocument);
                        break;
                    }
                    default: {
                        break;
                    }
                }


            }
        } catch (ParserConfigurationException e) {
            System.err.println("ERROR!! in "+filename);

        } catch (SAXException e) {
            System.err.println("ERROR!! in "+filename);
        } catch (IOException e) {
            System.err.println("ERROR!! in "+filename);
        } catch (NullPointerException e) {
            System.err.println("ERROR!! in "+filename);
        } catch (Exception ex) {
            System.err.println("ERROR!! in "+filename);
        }
        return anyDocumentList;
    }

    public static List<structures.Document> parseDocument(String text) {
        //Get the DOM Builder Factory

        List<structures.Document> anyDocumentList = new ArrayList<>();
        InputStream stream = inputStream(text);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = null;
        try {
            builder = factory.newDocumentBuilder();
            Document document = builder.parse(stream);
            NodeList nodeList = document.getElementsByTagName("ROOT");

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node item = nodeList.item(i);
                Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
                String attributeValue = itemAttribute.getNodeValue();

                switch (attributeValue) {

                    case "A": {
                        Article anyDocument = parseArticle(item);
                        if (!anyDocument.isEmpty())
                            anyDocumentList.add(anyDocument);
                        break;
                    }
                    case "B": {
                        Blog anyDocument = parseBlog(item);
                        if (!anyDocument.isEmpty())
                            anyDocumentList.add(anyDocument);
                        break;
                    }
                    case "T": {
                        List<Tweet> anyDocument = parseTweet(item);
                        anyDocumentList.addAll(anyDocument);
                        break;
                    }
                    case "R":{
                        List<Reuters> anyDocument = parseReuters(item);
                        anyDocumentList.addAll(anyDocument);

                    }
                    default: {
                        break;
                    }
                }


            }
        } catch (ParserConfigurationException e) {
            System.err.println("ERROR!!");

        } catch (SAXException e) {
            System.err.println("ERROR!!");
        } catch (IOException e) {
            System.err.println("ERROR!!");
        } catch (NullPointerException e) {
            System.err.println("ERROR!!");
        } catch (Exception ex) {
            System.err.println("ERROR!!");
        }
        return anyDocumentList;
    }


    public static Blog parseBlog(Node node) {
        //Get the DOM Builder Factory

        Blog blogDocument = new Blog("", "");


        NodeList nodeList = ((Element) node).getElementsByTagName("RESULT");
        blogDocument = new Blog();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node item = nodeList.item(i);
            Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
            String attributeValue = itemAttribute.getNodeValue();

            switch (attributeValue) {
                case "GENRE": {
                    String genre = item.getTextContent();
                    if (genre != null & !genre.isEmpty())
                        blogDocument.setGenre(genre.trim().toLowerCase(trLocale));
                    break;
                }
                case "AUTHORNAME": {
                    String authorName = item.getTextContent();
                    if (authorName != null && !authorName.isEmpty())
                        blogDocument.setAuthor(authorName.trim().toLowerCase(trLocale));
                    break;
                }
                case "ARTICLETITLE": {
                    String title = item.getTextContent();
                    blogDocument.setTitle(title.trim());
                    break;
                }
                case "ARTICLETEXT": {
                    String txt = item.getTextContent();
                    blogDocument.setText(txt.trim());
                    break;
                }
                case "ARTICLEPARAGRAPH": {
                    String par = item.getTextContent();
                    blogDocument.addParagraph(par.trim());
                }
            }
        }

        return blogDocument;
    }


    public static Article parseArticle(Node node) {
        //Get the DOM Builder Factory

        Article articleDocument = null;

        NodeList nodeList = ((Element) node).getElementsByTagName("RESULT");
        articleDocument = new Article();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element item = (Element) nodeList.item(i);
            Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
            String attributeValue = itemAttribute.getNodeValue();

            switch (attributeValue) {

                case "AUTHORNAME": {
                    String authorName = item.getTextContent();
                    if (authorName != null && !authorName.isEmpty()) {
                        int index = authorName.indexOf("|");
                        if (index > 0) {
                            authorName = authorName.substring(0, index).trim().replaceAll("\n","");
                        }
                        articleDocument.setAuthor(authorName.toLowerCase(trLocale));
                    }
                    break;
                }
                case "ARTICLETITLE": {
                    String title = item.getTextContent();
                    articleDocument.setTitle(title);
                    break;
                }
                case "GENRE": {
                    String genre = item.getTextContent();
                    articleDocument.setGenre(genre.trim());
                    break;
                }
                case "ARTICLETEXT": {
                    articleDocument.setText(item.getTextContent());
                    break;
                }
                case "ARTICLEPARAGRAPH": {
                    articleDocument.addParagraph(item.getTextContent());
                    break;
                }

            }


        }


        return articleDocument;

    }

    public static List<Tweet> parseTweet(Node node) {
        //Get the DOM Builder Factory


        Tweet tweetDocument = null;
        List<Tweet> tweetList = new ArrayList<>();
        NodeList nodeList = ((Element) node).getElementsByTagName("RESULT");

        for (int i = 0; i < nodeList.getLength(); i++) {
            Element item = (Element) nodeList.item(i);
            Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
            String attributeValue = itemAttribute.getNodeValue();

            switch (attributeValue) {
                case "TWEET": {
                    if (tweetDocument != null) {
                        tweetList.add(tweetDocument);
                        tweetDocument = new Tweet();
                        break;
                    } else {
                        tweetDocument = new Tweet();
                    }
                }

                case "AUTHORNAME": {
                    String authorName = item.getTextContent();
                    if (authorName != null)
                        tweetDocument.setAuthor(authorName.toLowerCase(trLocale));
                    break;
                }
                case "TWEETTEXT": {
                    String tweet = item.getTextContent();
                    if (tweet != null)
                        tweetDocument.setText(tweet);
                    break;
                }
            }
        }

        if (tweetDocument != null) {
            tweetList.add(tweetDocument);
        }

        return tweetList;

    }

    public static List<Reuters> parseReuters(Node node){
        Reuters reutersDocument = null;
        List<Reuters> reutersList = new ArrayList<>();
        NodeList nodeList = ((Element) node).getElementsByTagName("RESULT");

        for (int i = 0; i < nodeList.getLength(); i++) {
            Element item = (Element) nodeList.item(i);
            Node itemAttribute = item.getAttributes().getNamedItem("LABEL");
            String attributeValue = itemAttribute.getNodeValue();

            switch (attributeValue) {
                case "ARTICLE": {
                    if (reutersDocument != null) {
                        reutersList.add(reutersDocument);
                        reutersDocument = new Reuters();
                        break;
                    } else {
                        reutersDocument = new Reuters();
                        break;
                    }
                }

                case "TOPIC": {
                    String topicName = item.getTextContent();
                    if (topicName != null) {
                        topicName = topicName.trim();
                        reutersDocument.addTopic(topicName.toLowerCase(trLocale));
                    }
                    break;
                }
                case "ARTICLETEXT": {
                    String text = item.getTextContent();
                    if (text != null)
                        reutersDocument.setText(text);
                    break;
                }
            }
        }

        if (reutersDocument != null) {
            reutersList.add(reutersDocument);
        }

        return reutersList;
    }


    public static void main(String[] args) {
        String text = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<RESULT>\n" +
                "  <to> Tove</to>\n" +
                " <ROOT>Jani</ROOT>\n" +
                "  <heading>Reminder</heading>\n" +
                "  <body>Don't forget me this weekend!</body>\n" +
                "</RESULT>";

        XMLParser parser = new XMLParser();
        text = clean(text);
        List<structures.Document> article = parseDocument(text);


    }

}
