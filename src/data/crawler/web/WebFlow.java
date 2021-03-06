package data.crawler.web;

import data.document.Document;
import data.document.Document$;
import options.Resources;
import org.apache.avro.generic.GenericData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wolf on 02.07.2015.
 */
public class WebFlow implements Serializable, Callable<Boolean> {
    private String folder;
    private WebTemplate mainTemplate;

    public WebFlow(WebTemplate mainTemplate) {
        this.mainTemplate = mainTemplate;
        this.folder = mainTemplate.getFolder();

    }

    public WebFlow(WebTemplate mainTemplate, String folder) {
        this.mainTemplate = mainTemplate;
        this.folder = folder;
        this.mainTemplate.setFolder(folder + mainTemplate.getFolder());

    }

    @Override
    public Boolean call() throws Exception {
        execute();
        return true;
    }

    public WebDocument execute() {
        //Download template
        //Extract patterns
        //Pass seedlist to next template as urls if it is a url
        WebDocument mainDocument = mainTemplate.execute();

        return mainDocument;
    }


    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Gazete Oku, Fotomac">
    public static WebFlow buildGazeteBatchFlow() {

        //Main Download
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "article-links", "http://www.gazeteoku.com");
        String mainSeed = "http://www.gazeteoku.com/tum-yazarlar.html";
        mainTemplate.addSeed(mainSeed);

        LookupPattern yazarPattern = new LookupPattern(LookupOptions.URL, LookupOptions.MAINPAGE, "<li class=\"clearfix\">", "</li>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORLINK, "<a\\shref\\=\"", "\" title").setNth(1));

        mainTemplate.setMainPattern(yazarPattern);

        //Link Download
        WebTemplate linkTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "author-links", "http://www.gazeteoku.com", "?page=")
                .setNextPageStart(1)
                .setNextPageSize(2);
        LookupPattern patternLinkArticle = new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLE, "<div\\sclass=\"syList\">", "</div>");
        LookupPattern patternLink = new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLELINK, "<a href=\"", "\"\\s");

        patternLinkArticle.addPattern(patternLink);
        linkTemplate.setMainPattern(patternLinkArticle);

        //Article Download
        WebTemplate articleTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "article-text", "http://www.gazeteoku.com")
                .setType(Resources.DocumentResources$.MODULE$.ARTICLE());
        LookupPattern nameLookup = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.AUTHORNAME, "<span\\sclass=\"yiyName\">", "</span>")
                .setStartMarker("<span")
                .setEndMarker("</span>");

        LookupPattern titleLookup = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETITLE, "<div\\sclass=\"quotesEnd\">", "</div>");
        LookupPattern genreeLookup = new LookupPattern(LookupOptions.TEXT, LookupOptions.GENRE, LookupOptions.GENREPOLITICS);
        LookupPattern paragraphLookup = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>");
        LookupPattern contentLookup = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sclass=\"articleBlock\\sclearfix\">", "</div>")
                .setStartMarker("<div")
                .setEndMarker("</div>")
                .addPattern(paragraphLookup);

        LookupPattern articleLookup = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.CONTAINER, "<div\\sclass=\"ydbLert\\s\">", "</div>")
                .setStartMarker("<div")
                .setEndMarker("</div")
                .addPattern(nameLookup)
                .addPattern(titleLookup)
                .addPattern(genreeLookup)
                .addPattern(contentLookup);

        articleTemplate.setMainPattern(articleLookup);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.ARTICLEDOC());

        mainTemplate.addNext(linkTemplate, LookupOptions.AUTHORLINK);
        linkTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);


        WebFlow flow = new WebFlow(mainTemplate);
        return flow;
    }


    public static WebFlow buildFotomacBatchFlow() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "fotomac", "http://www.fotomac.com.tr");
        String mainSeed = "http://www.fotomac.com.tr/yazarlar/tumyazarlar";
        mainTemplate.addSeed(mainSeed);
        mainTemplate.setNextPageSuffix("?tc=107&page=");

        //Download Author Archive
        LookupPattern yazarPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<ul\\sclass=\"writerList\">", "</ul>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.AUTHOR, "<li>", "</li>")
                        .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.AUTHORLINK, "<a href=\"", "\"\\sclass=\"article\">")))
                .setStartEndMarker("<ul", "</ul>");

        mainTemplate.setMainPattern(yazarPattern);

        //Download Writing Links for each author archive
        WebTemplate linkTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "article-links", "http://www.fotomac.com.tr/");
        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLE, "<ul\\sclass=\"writerArchive\">", "</ul>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a href=\"", "\"\\s"));


        linkTemplate.setMainPattern(linkPattern);

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.ARTICLEDIRECTORY, "article-text", "http://www.fotomac.com.tr")
                .setCharset("Windows-1254")
                .setType(Resources.DocumentModel$.MODULE$.ARTICLEDOC());
        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<div class=\"detail news\">", "</div>")
                .setStartMarker("<div")
                .setEndMarker("</div>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<span class=\"title\">", "</span>"))
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRESPORTS))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sid=\"NewsTitle\">", "</h1>"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETEXT, "<p\\sid=\"NewsDescription\">", "</p>")
                        .setStartMarker("<p").setEndMarker("</p>"));

        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.ARTICLEDOC());

        linkTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        mainTemplate.addNext(linkTemplate, LookupOptions.AUTHORLINK);


        WebFlow flow = new WebFlow(mainTemplate);

        return flow;

    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Build for Blogs: Webrazzi, Gezenler Klubu, Beshafliler">


    public static WebFlow buildForWebrazzi() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", "http://webrazzi.com/");

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLE, "<div class=\"post-title\">", "</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "href=\"", "\"\\s"));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("page/");
        mainTemplate.setNextPageSize(5);
        mainTemplate.setNextPageStart(2);
        mainTemplate.addSeed("http://webrazzi.com/");

        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<article(.*?)itemtype=\"http://schema.org/BlogPosting\"(.*?)>", "</article>")
                .setStartEndMarker("<article", "</article>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1 itemprop=\"headline\">", "</h1>"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.GENRE, "<a href=\"/kategori/", "/\"\\s"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a href=\"/author/.*?>", "</a>"))
                .addPattern(new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sclass=\"post-content\">", "</div>")
                        .setStartEndMarker("<div", "</div")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", "http://webrazzi.com");
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }

    public static WebFlow buildForGezenlerKulubu() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLE, "<article\\sclass=\"post\".*?>", "</article>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a href=\"", "\"\\stitle="));

        mainTemplate.setMainPattern(linkPattern);

        mainTemplate.setNextPageSuffix("?page=")
                .setNextPageSize(57)
                .setNextPageStart(2)
                .addSeed("http://www.cokgezenlerkulubu.com/");

        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<div\\sclass=\"postdetail\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1>", "</h1>").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.GENRE, "travel"))
                .addPattern(new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sclass=\"text\">", "</div>")
                        .setStartEndMarker("<div", "</div")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")))
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.AUTHORNAME, "<div\\sclass=\"editor\"> ", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<span>", "</span>")));


        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", "http://www.cokgezenlerkulubu.com/");
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }

    public static WebFlow buildForBesHarfliler() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLE, "<div\\sclass=\"grid_8\\somega\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref=\"", "\">").setNth(0));

        mainTemplate.setMainPattern(linkPattern)
                .setNextPageSuffix("page/")
                .setNextPageSize(5)
                .setNextPageStart(2)
                .addSeed("culture", "http://www.5harfliler.com/category/kultur/")
                .addSeed("history", "http://www.5harfliler.com/category/tarih/")
                .addSeed("art", "http://www.5harfliler.com/category/sanat/")
                .addSeed("social", "http://www.5harfliler.com/category/meydan/");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<section>", "</section>")
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.AUTHORNAME, "<div\\sclass=\"grid_4\\sauthor.*?>", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<h5>", "</h5>").setNth(0)))
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.ARTICLE, "<div\\sclass=\"grid_12\\sarticle(.*?)>", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .setNth(0)
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1>", "</h1>").setNth(0))
                        .addPattern(new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sclass=\"post_content\">", "</div>")
                                .setStartEndMarker("<div", "</div")
                                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>"))));


        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", "http://www.5harfliler.com/");
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);


        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Hayrola, Sonbirseyler Blog">
    public static WebFlow buildForHayrola() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", "http://hayro.la");

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<ul class=\"archive-list\">", "</ul>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINKCONTAINER, "<li>", "</li>")
                        .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a href=\"", "\">")
                                .setNth(1)));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("page/");
        mainTemplate.setNextPageSize(5);
        mainTemplate.setNextPageStart(2);
        mainTemplate.addSeed("interesting", "http://hayro.la/kategori/ilgi-cekici/");
        mainTemplate.addSeed("woman", "http://hayro.la/kategori/kadin/");
        mainTemplate.addSeed("technology", "http://hayro.la/kategori/teknoloji/");
        mainTemplate.addSeed("funny", "http://hayro.la/kategori/komik/");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sid=\"content-wrapper\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sclass=\"post-title\".*?>", "</h1>").setNth(1))
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.AUTHORNAME, "<span\\sclass=\"author_name\".*?>", "</span>")
                        .setStartEndMarker("<span", "</span>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a\\shref=.*?>", "</a>")))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sid=\"content_without_ad\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", "http://hayro.la");
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }

    public static WebFlow buildForSonBirseyler() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<div class=\"td-pb-span8 td-main-content\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINKCONTAINER, "<h3 itemprop=\"name\" class=\"entry-title td-module-title\">", "</h3>")
                        .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a.*?href=\"", "\"").setNth(1)));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("page/");
        mainTemplate.setNextPageSize(5);
        mainTemplate.setNextPageStart(2);
        mainTemplate.addSeed("social", "http://www.sonbisey.com/category/gundem/");
        mainTemplate.addSeed("funny", "http://www.sonbisey.com/category/eglencelibiseyler/");
        mainTemplate.addSeed("entertainment", "http://www.sonbisey.com/category/yararlibiseyler/");
        mainTemplate.addSeed("interesting", "http://www.sonbisey.com/category/baskabiseyler/");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<article id=\"post-\\d+.*?>", "</article>")
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1 itemprop=\"name\" class=\"entry-title\">", "</h1>").setNth(1))
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.AUTHORNAME, "<div class=\"td-post-author-name\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a.*?>", "</a>")))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div class=\"td-post-content\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>").setNth(2).setMth(100)));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }

    public static WebFlow buildForSosyalRadar() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<h2\\sclass=\"post-title\">", "</h2>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref=\"", "\"").setNth(0));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("/page/");
        mainTemplate.setNextPageSize(10);
        mainTemplate.setNextPageStart(2);
        mainTemplate.addSeed("social-media", "http://www.sosyalradar.com/k/sosyal-medya");
        mainTemplate.addSeed("web", "http://www.sosyalradar.com/k/web");
        mainTemplate.addSeed("technology", "http://www.sosyalradar.com/k/teknoloji");
        mainTemplate.addSeed("commerce", "http://www.sosyalradar.com/k/e-ticaret");
        mainTemplate.addSeed("enterprise", "http://www.sosyalradar.com/k/girisim");
        mainTemplate.addSeed("qa", "http://www.sosyalradar.com/k/soru-cevap");
        mainTemplate.addSeed("design", "http://www.sosyalradar.com/k/tasarim");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<article\\sclass=\".*?>", "</article>")
                //.setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sclass=\"post-title\">", "</h1>").setNth(1))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a\\shref=.*?rel=\"author\">", "</a>").setNth(1))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sclass=\"entry\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }


    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Egonomik Blog">
    public static WebFlow buildForEgonomic() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<div\\sclass\\=\"thumbnail\\soverlay\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref\\=\"", "\"").setNth(0));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("/page/");
        mainTemplate.setNextPageSize(5);
        mainTemplate.setNextPageStart(2);

        mainTemplate.addSeed("cinema", "http://www.egonomik.com/category/sinema");
        mainTemplate.addSeed("pc-games", "http://www.egonomik.com/category/oyunlar/");
        mainTemplate.addSeed("web-design", "http://www.egonomik.com/category/web-tasarim");
        mainTemplate.addSeed("internet", "http://www.egonomik.com/category/internet");
        mainTemplate.addSeed("fun", "http://www.egonomik.com/category/eglencelik/");

        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sid=\"content\"", "</article>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.CONTAINER, "<span\\sclass=\"entry-cats\">", "</span>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.GENRE, "<h5>", "</h5>")))
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.CONTAINER, "<span\\sclass=\"author\">", "</span>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a\\shref.*?>", "</a>")))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sclass=\"entry-title\">", "</h1>"))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sclass=\"entry-content\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());
        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);

        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;


    }
    //</editor-fold>
    ///////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="AcikEkonomi,Avaz Blog">
    public static WebFlow buildForAcikEconomy() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<h1\\sclass=\"entry-title\">", "</h1>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref=\"", "\""));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setNextPageSuffix("/page/");
        mainTemplate.setNextPageSize(6);
        mainTemplate.setNextPageStart(2);
        mainTemplate.addSeed("economy", "acikekonomi.com");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sid=\"content\"(.*?)>", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sclass=\"entry-title\">", "</h1>").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<a\\sclass=\"author\"(.*?)>", "</a>"))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sclass=\"entry-content\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")
                                .setNth(1)
                                .setMth(100)));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }

    public static WebFlow buildForAvazAvaz() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<div\\sclass=\"brick-media\">", "</div>")
                .setStartEndMarker("<div","</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref=\"", "\""));

        mainTemplate.setMainPattern(linkPattern);
        /*mainTemplate.setNextPageSuffix("/page/");
        mainTemplate.setNextPageSize(6);
        mainTemplate.setNextPageStart(2);*/
        mainTemplate.addSeed("music","file:///home/wolf/Documents/java-projects/AuthorIdentification/resources/avaz.html");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sclass=\"content-wrap\">", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.LOOKUP, LookupOptions.GENRE, LookupOptions.GENRE))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1\\sclass=\"entry-title\\spage-title\">", "</h1>"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<span\\sclass=\"author\\svcard\\sicon-user\">", "</span>"))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sclass=\"entry-content\">", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }


    public static WebFlow buildForMilliyetBlog() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-links", LookupOptions.EMPTYDOMAIN);

        LookupPattern linkPattern = new LookupPattern(LookupOptions.URL, LookupOptions.CONTAINER, "<div\\sclass=\"haberDetay\">", "</div>")
                .setStartEndMarker("<div","</div>")
                .addPattern(new LookupPattern(LookupOptions.URL, LookupOptions.ARTICLELINK, "<a\\shref=\"", "\"").setNth(0));

        mainTemplate.setMainPattern(linkPattern);
        mainTemplate.setDomain("http://blog.milliyet.com.tr/");
        mainTemplate.setNextPageSuffix("Page=");
        mainTemplate.setNextPageSize(59);
        mainTemplate.setNextPageStart(1);
        mainTemplate.addSeed("http://blog.milliyet.com.tr/BlogListe/?Status=&Sort=&KategoriNo=&");


        LookupPattern articlePattern = new LookupPattern(LookupOptions.ARTICLE, LookupOptions.ARTICLETEXT, "<div\\sid=\"_middle_content(.*?)>", "</div>")
                .setStartEndMarker("<div", "</div>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.GENRE, "<dd>","</dd>").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETITLE, "<h1>", "</h1>").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<h4>", "</h4>").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLETEXT, "<div\\sid=\"BlogDetail\"(.*?)>", "</div>")
                        .setStartEndMarker("<div", "</div>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEPARAGRAPH, "<p>", "</p>")));

        WebTemplate articleTemplate = new WebTemplate(LookupOptions.BLOGDIRECTORY, "blog-text", LookupOptions.EMPTYDOMAIN);
        articleTemplate.setMainPattern(articlePattern);
        articleTemplate.setType(Resources.DocumentModel$.MODULE$.BLOGDOC());

        mainTemplate.addNext(articleTemplate, LookupOptions.ARTICLELINK);
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;

    }






    //</editor-fold>
    ///////////////////////////////////////////////////////////

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Reuters">
    public static WebFlow buildForReuters() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.REUTERSDIRECTORY, "reuters-article", LookupOptions.EMPTYDOMAIN);

        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<REUTERS TOPICS=\"YES\".*?>", "</REUTERS>")
                .setStartEndMarker("<REUTERS", "</REUTERS>")
                .addPattern(new LookupPattern(LookupOptions.SKIP, LookupOptions.CONTAINER, "<TOPICS>", "</TOPICS>")
                        .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.TOPIC, "<D>", "</D>")))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETEXT, "<BODY>", "</BODY"));


        mainTemplate.addFolder(LookupOptions.REUTERSSOURCEDIRECTORY);

        mainTemplate.setMainPattern(articlePattern);
        mainTemplate.setType(Resources.DocumentModel$.MODULE$.REUTERSDOC());
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="PAN2011">

    public static WebFlow buildForPAN2011Large() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.PANLARGESOURCEDIRECTORY, "LargeTrain", LookupOptions.EMPTYDOMAIN);

        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<text.*?", "</text>")
                .setStartEndMarker("<text", "</text>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEID, "file=\"", "\">").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<author id=\"", "\"/>"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETEXT, "<body>", "</body>")
                        .setRegex(new String[]{"&", "<NAME/>", "<.*?>", ">", "<"}).setReplaces(new String[]{"&amp;", "NamedEntity", "", "&gt;", "&lt;"}));

        mainTemplate.addFolder(LookupOptions.PANLARGEDIRECTORY);
        mainTemplate.setMainPattern(articlePattern);
        mainTemplate.setType(Resources.DocumentModel$.MODULE$.PANDOC());
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }

    public static WebFlow buildForPAN2011Small() {
        WebTemplate mainTemplate = new WebTemplate(LookupOptions.PANSMALLSOURCEDIRECTORY, "SmallTrain", LookupOptions.EMPTYDOMAIN);

        LookupPattern articlePattern = new LookupPattern(LookupOptions.CONTAINER, LookupOptions.ARTICLE, "<text.*?", "</text>")
                .setStartEndMarker("<text", "</text>")
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLEID, "file=\"", "\">").setNth(0))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.AUTHORNAME, "<author id=\"", "\"/>"))
                .addPattern(new LookupPattern(LookupOptions.TEXT, LookupOptions.ARTICLETEXT, "<body>", "</body>")
                        .setRegex(new String[]{"&", "<NAME/>", "<.*?>", ">", "<"}).setReplaces(new String[]{"&amp;", "NamedEntity", "", "&gt;", "&lt;"}));

        mainTemplate.addFolder(LookupOptions.PANSMALLDIRECTORY);
        mainTemplate.setMainPattern(articlePattern);
        mainTemplate.setType(Resources.DocumentModel$.MODULE$.PANDOC());
        WebFlow webFlow = new WebFlow(mainTemplate);
        return webFlow;
    }


    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////


    public static void submit(ExecutorService service, final WebFlow flow) {
        service.submit(new Runnable() {
            @Override
            public void run() {
                flow.execute();
            }
        });
    }

    public static void batchSubmit(ExecutorService service, List<WebFlow> webFlows){
        try {
            service.invokeAll(webFlows);
            service.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        ExecutorService service = Executors.newFixedThreadPool(5);

        WebFlow flowWebrazzi = buildForWebrazzi();
        WebFlow flowBesHarfliler = buildForBesHarfliler();
        WebFlow flowGezenler = buildForGezenlerKulubu();
        WebFlow flowHayrola = buildForHayrola();
        WebFlow flowSonBirseyler = buildForSonBirseyler();
        WebFlow flowEgonomic = buildForEgonomic();
        //WebFlow flowAcikEconomy = buildForAcikEconomy();
        WebFlow flowAvaz = buildForAvazAvaz();
        WebFlow flowMilliyet = buildForMilliyetBlog();
        WebFlow flowGazeteOku = buildGazeteBatchFlow();

        /*submit(service,flowWebrazzi);
        submit(service,flowBesHarfliler);
        submit(service,flowGezenler);
        submit(service, flowHayrola);
        submit(service,flowEgonomic);
        //submit(service, flowAcikEconomy);
        submit(service, flowAvaz);
        submit(service, flowMilliyet);
        submit(service, flowSonBirseyler);
        submit(service, flowGazeteOku);
        service.shutdown();
        */

        List<WebFlow> flows = Arrays.asList(flowWebrazzi, flowBesHarfliler, flowGezenler,
                flowHayrola, flowSonBirseyler,flowEgonomic,flowAvaz, flowMilliyet,flowGazeteOku);

        batchSubmit(service, flows);

        /*WebFlow flowTrain = buildForPAN2011Small();
        flowTrain.execute();*/

       /* WebFlow flowGazeteOku = buildGazeteBatchFlow();
        flowGazeteOku.execute();*/

    }

}
