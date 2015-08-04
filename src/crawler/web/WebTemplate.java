package crawler.web;

import operations.TextFile;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by wolf on 02.07.2015.
 */
public class WebTemplate implements Serializable {
    //Extraction Template
    public static String FILEPREFIX = "file://";
    private List<String> seedList;
    private Map<String, String> seedMap;
    private LookupPattern mainPattern;
    private String name;
    private String domain;
    private String folder;
    private String type;
    private String nextPageSuffix;
    private Integer nextPageSize;
    private Integer nextPageStart;

    private String charset = "UTF-8";
    private Map<String, WebTemplate> nextMap;

    public WebTemplate(String folder, String name, String domain) {
        this.name = name;
        this.folder = folder;
        this.domain = domain;
        this.seedList = new ArrayList<>();
        this.seedMap = new HashMap<>();
        this.nextMap = new HashMap<>();
        this.nextPageSize = 2;
        this.nextPageStart = 2;

    }

    public WebTemplate(String folder, String name, String domain, String nextPageSuffix) {
        this(folder, name, domain);
        this.nextPageSuffix = nextPageSuffix;
    }

    public String getCharset() {
        return charset;
    }

    public WebTemplate setCharset(String charset) {
        this.charset = charset;
        return this;
    }

    public Integer getNextPageStart() {
        return nextPageStart;
    }

    public WebTemplate setNextPageStart(Integer nextPageStart) {
        this.nextPageStart = nextPageStart;
        return this;
    }

    public Integer getNextPageSize() {
        return nextPageSize;
    }

    public WebTemplate setNextPageSize(Integer nextPageSize) {
        this.nextPageSize = nextPageSize;
        return this;
    }

    public WebTemplate setDomain(String domain) {
        this.domain = domain;
        return this;
    }

    public WebTemplate setName(String name) {
        this.name = name;
        return this;
    }

    public String getNextPageSuffix() {
        return nextPageSuffix;
    }

    public WebTemplate setNextPageSuffix(String nextPageSuffix) {
        this.nextPageSuffix = nextPageSuffix;
        return this;
    }

    public LookupPattern getMainPattern() {
        return mainPattern;
    }

    public WebTemplate setMainPattern(LookupPattern mainPattern) {
        this.mainPattern = mainPattern;
        return this;
    }

    public String getDomain() {
        return domain;
    }

    public String getType() {
        return type;
    }

    public WebTemplate setType(String type) {
        this.type = type;
        return this;
    }

    public WebTemplate addNext(WebTemplate nextTemplate, String label) {
        this.nextMap.put(label, nextTemplate);
        return this;
    }

    public boolean nextEmpty() {
        return this.nextMap.isEmpty();
    }

    public WebTemplate addSeed(String seed) {
        if (!seedList.contains(seed))
            seedList.add(seed);

        return this;
    }

    public WebTemplate addSeed(String genre, String seed) {

        seedMap.put(seed, genre);
        return addSeed(seed);
    }

    public WebTemplate addFolder(String folder){
        File[] files = (new File(folder)).listFiles();
        for(File file:files){
            addSeed(FILEPREFIX+file.getPath());
        }

        return this;
    }

    public List<String> getSeedList() {
        return seedList;
    }

    public String getName() {
        return name;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public WebTemplate getNextMap(String label) {
        WebTemplate webTemplate = nextMap.get(label);
        webTemplate.getSeedList().clear();
        return webTemplate;
    }

    public Iterator<String> getNextIterator() {
        return nextMap.keySet().iterator();
    }

    public void saveXML(List<WebDocument> documentList) {
        for (WebDocument document : documentList) document.saveAsFlatXML();
    }

    public WebDocument execute() {
        //Download template
        //Extract patterns
        //Pass seedlist to next template as urls if it is a url

        //Download current template
        List<WebDocument> documentList = download();
        WebDocument mainDocument = new WebDocument(folder, name).setType(type);


        if (nextEmpty()) {
            //Leaf template
            mainDocument.addWebFlowResult(documentList);
            saveXML(documentList);
        } else {
            //Non leaf template
            Collections.shuffle(documentList);
            for (WebDocument document : documentList) {
                Iterator<String> templateIter = getNextIterator();
                Map<String, String> properties = document.getProperties();
                String genre = properties.get(LookupOptions.GENRE);
                while (templateIter.hasNext()) {
                    String templateLink = templateIter.next();
                    WebTemplate template = getNextMap(templateLink);

                    for (LookupResult result : document.getLookupFinalList(templateLink)) {

                        template.addSeed(genre,domain + result.getText());

                    }

                    WebDocument webFlowResultList = template.execute();
                    mainDocument.addWebFlowResult(webFlowResultList);
                }
            }

            saveXML(documentList);
        }


        return mainDocument;
    }

    public List<WebDocument> download() {
        List<WebDocument> htmlDocumentList = new ArrayList<>();

        if (nextPageSuffix != null && !nextPageSuffix.isEmpty()) {
            List<String> nextPageSeeds = new ArrayList<>();
            Map<String, String> nextPageSeedGenre = new HashMap<>();

            Collections.shuffle(seedList);

            for (String url : seedList) {
                for (int i = nextPageStart; i <= nextPageSize; i++) {
                    String newUrl = url + nextPageSuffix + i;
                    if (!nextPageSeeds.contains(newUrl)) {
                        nextPageSeeds.add(newUrl);
                        nextPageSeedGenre.put(newUrl,seedMap.get(url));
                    }
                }
            }

            for (String url : nextPageSeeds) {

                if (!seedList.contains(url)) {
                    seedList.add(url);
                    seedMap.put(url, nextPageSeedGenre.get(url));
                }
            }
        }

        for (int i = 0; i < seedList.size(); i++) {
            String url = seedList.get(i);
            String value = downloadFile(url, charset);

            WebDocument document = new WebDocument(folder, name, url);
            document.setFetchDate(new Date());
            document.setText(value);
            document.setDomain(domain);
            document.setType(type);
            document.putProperty(LookupOptions.GENRE, seedMap.get(url));
            document.setIndex(i);
            extract(document);

            htmlDocumentList.add(document);
        }


        return htmlDocumentList;
    }


    public void extract(WebDocument html) {

        List<LookupResult> results = mainPattern.getResult(html.getProperties(),html.getText());
        html.setLookupResultList(results);

    }

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Utilities">

    private String downloadFile(String address, String charset){
        if(address.startsWith(FILEPREFIX)){
            String filename = address.substring(address.indexOf(FILEPREFIX)+FILEPREFIX.length());
            System.out.println("Downloading... '" + address + "'");
            return new TextFile(filename, charset).readFullText();
        }
        else
        {
            System.out.println("Downloading... '" + address + "'");
            return downloadPage(address,charset);
        }
    }

    private String downloadPage(String address, String charset) {
        String text = "";


        if (address != null && !address.isEmpty()) {
            address = address.replaceAll("\\s", "%20");
            address = address.startsWith("http://") ? address : "http://" + address;

            Request request = Request.Get(address);
            if (request != null) {
                try {
                    //request.socketTimeout(160).connectTimeout(1500);
                    request.addHeader("user-agent", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9) Gecko/2008052906 Firefox/3.0");
                    request.addHeader("accept", "image/gif, image/jpg, */*");
                    request.addHeader("connection", "keep-alive");
                    request.addHeader("accept-encoding", "gzip,deflate,sdch");
                    Response response = request.execute();
                    Thread.sleep(20);
                    if (response != null) {
                        HttpResponse responseContent = response.returnResponse();

                        HttpEntity entity = responseContent.getEntity();
                        ContentType contentType = ContentType.getOrDefault(entity);

                        InputStream stream = entity.getContent();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, charset));
                        text = "";
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            text += line + "\n";
                        }
                    }
                } catch (IOException ex) {
                    System.out.println("Error in url '" + address + "'");

                } catch (InterruptedException ex) {
                    Logger.getLogger(WebTemplate.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return text;
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

}
