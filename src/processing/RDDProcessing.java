package processing;


import language.boundary.SentenceML;
import org.apache.spark.api.java.function.*;
import processing.structures.stats.DatasetStats;
import processing.structures.docs.*;
import processing.utils.XMLParser;
import processing.structures.summary.PANDoc;
import util.PrintBuffer;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import processing.comparators.TupleSizeComparator;
import scala.Tuple2;

import processing.structures.summary.ReutersDoc;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by wolf on 09.07.2015.
 */
public class RDDProcessing implements Serializable {

    private String TESTDIRECTORY = "resources/test/blog-text*";
    private String BLOGDIRECTORY = "resources/blogs/blog-text*";
    private String ARTICLEDIRECTORY = "resources/articles/article-text*";
    private String REUTERSDIRECTORY = "resources/reuters/reuters-article*";

    private String TWEETDIRECTORY = "resources/twitter/twitter*";
    public static String PANBINARYLARGEDIRECTORY = "resources/pan/binary-large";
    public static String PANLARGEDIRECTORY = "resources/pan/sources-large/*";
    public static String PANBINARYSMALLDIRECTORY = "resources/pan/binary-small";
    public static String PANSMALLDIRECTORY = "resources/pan/sources-small/*";

    public RDDProcessing() {

    }


    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Utility">
    public static String documentGenre(Document v1) {

        if (v1.isBlog()) {
            return ((Blog) v1).getGenre();
        } else if (v1.isArticle())
            return ((Article) v1).getGenre();
        else
            return "TWEET";
    }

    public static <X> void increment(Map<X, Tuple2<X, Double>> tuple2Map, X label) {
        if (tuple2Map.containsKey(label)) {
            tuple2Map.put(label, increment(tuple2Map.get(label)));
        } else {
            tuple2Map.put(label, new Tuple2<X, Double>(label, 1D));
        }
    }

    public static <X> Tuple2<X, Double> increment(Tuple2<X, Double> tuple) {
        X label = tuple._1();
        double value = tuple._2() + 1;
        return new Tuple2<>(label, value);
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Dataset Statitics">
    public DatasetStats buildPanStats(JavaSparkContext sparkContext, String directory){
        JavaRDD<PANDoc> panRDD = panRDD(loadXML(sparkContext, directory));
        DatasetStats stats = new DatasetStats();
        //Distinct Authors
        //Average Number of documents
        //Each author's number of documents (min-max)

        JavaPairRDD<String, Iterable<PANDoc>> authorDocRDD = panRDD.mapToPair(new PairFunction<PANDoc, String, PANDoc>() {
            @Override
            public Tuple2<String, PANDoc> call(PANDoc panDoc) throws Exception {
                return new Tuple2<String, PANDoc>(panDoc.getAuthorid(),panDoc);
            }
        }).groupByKey();

        JavaPairRDD<String,Long> authorCountRDD = authorDocRDD.mapValues(new Function<Iterable<PANDoc>, Long>() {
            @Override
            public Long call(Iterable<PANDoc> v1) throws Exception {
                return new Long(IteratorUtils.toList(v1.iterator()).size());
            }
        });

        final TupleSizeComparator comparator = new TupleSizeComparator();
        Long authorCount = authorCountRDD.count();
        Long minInstanceCount = authorCountRDD.min(new TupleSizeComparator())._2();
        Long maxInstanceCount = authorCountRDD.max(new TupleSizeComparator())._2();

        Long totalInstanceCount = authorCountRDD.reduce(new Function2<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                return new Tuple2<String, Long>("ALL",v1._2()+ v2._2());
            }
        })._2();

        stats.setNumberOfClasses(authorCount);
        stats.setMinInstancePerClass(minInstanceCount);
        stats.setMaxInstancePerClass(maxInstanceCount);
        stats.setTotalInstancePerClass(totalInstanceCount);

        return stats;
    }
    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    //Eliminate duplicates, empty records save as RDD JSon
    //Loading Flat XML : files RDD

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Loading Documents">

    public JavaPairRDD<String, String> loadXML(JavaSparkContext sparkContext, String directory) {
        return sparkContext.wholeTextFiles(directory);
    }

    protected JavaPairRDD<String, String> loadXML(JavaSparkContext sparkContext, String[] directories) {
        //return loadXML(sparkContext, REUTERSDIRECTORY);
        //loadXML(sparkContext, BLOGDIRECTORY).union(loadXML(sparkContext, ARTICLEDIRECTORY));
        //.union(loadXML(sparkContext, TWEETDIRECTORY));
        if (directories.length == 0) return null;
        else {
            JavaPairRDD<String, String> xmlRDD = loadXML(sparkContext, directories[0]);
            for (int i = 1; i < directories.length; i++) {
                xmlRDD = xmlRDD.union(loadXML(sparkContext, directories[i]));
            }
            return xmlRDD;
        }
    }



    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////




    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="PAN Processing">

    public JavaRDD<PANDoc> panRDDLoad(JavaSparkContext sc){
        return sc.objectFile(PANBINARYLARGEDIRECTORY);
    }

    /**
     * Load XML Document as RDD, find unknown pairs, save them
     * @param sparkContext

     * @return
     */
    public JavaRDD<PANDoc> panLargeRDD(JavaSparkContext sparkContext) {
        JavaRDD<PANDoc> docJavaRDD = panRDD(loadXML(sparkContext, PANLARGEDIRECTORY));
        return docJavaRDD;
    }

    public JavaRDD<PANDoc> panSmallRDD(JavaSparkContext sparkContext) {
        JavaRDD<PANDoc> docJavaRDD = panRDD(loadXML(sparkContext, PANSMALLDIRECTORY));
        return docJavaRDD;
    }

    public JavaRDD<PANDoc> panRDDLargeSave(JavaSparkContext sparkContext) {
        JavaRDD<PANDoc> docJavaRDD = panRDD(loadXML(sparkContext, PANLARGEDIRECTORY));
        docJavaRDD.saveAsObjectFile(PANBINARYLARGEDIRECTORY);
        return docJavaRDD;
    }
    public JavaRDD<PANDoc> panRDDSmallSave(JavaSparkContext sparkContext) {
        JavaRDD<PANDoc> docJavaRDD = panRDD(loadXML(sparkContext, PANSMALLDIRECTORY));
        docJavaRDD.saveAsObjectFile(PANBINARYSMALLDIRECTORY);
        return docJavaRDD;
    }

    public JavaRDD<PANDoc> panRDDSortByDocFreq(JavaRDD<PANDoc> panDocRDD, final String filterTye, final int minimumDocs){
        //Group by each label
        //Sort by their doc frequency
        //Filter by minimum doc frequency

        JavaRDD<PANDoc> docs = panDocRDD.filter(new Function<PANDoc, Boolean>() {
            @Override
            public Boolean call(PANDoc panDoc) throws Exception {
                return panDoc.getDocid().startsWith(filterTye);
            }
        });

        JavaPairRDD<Double, Iterable<PANDoc>> pairRDD = docs.groupBy(new Function<PANDoc, Double>() {
            @Override
            public Double call(PANDoc v1) throws Exception {
                return v1.getLabel();
            }
        });

        JavaPairRDD<Double, Iterable<PANDoc>> filtered = pairRDD.filter(new Function<Tuple2<Double, Iterable<PANDoc>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Iterable<PANDoc>> v1) throws Exception {
                List<PANDoc> docList = IteratorUtils.toList(v1._2().iterator());
                return docList.size()>=minimumDocs;
            }
        });

        //sort by iterable size
        //first map and then sort
        /*JavaPairRDD<Integer, Iterable<PANDoc>> mapped = filtered.mapToPair(new PairFunction<Tuple2<Double, Iterable<PANDoc>>, Integer, Iterable<PANDoc>>() {
            @Override
            public Tuple2<Integer, Iterable<PANDoc>> call(Tuple2<Double, Iterable<PANDoc>> doubleIterableTuple2) throws Exception {
                Iterable<PANDoc> iterable = doubleIterableTuple2._2();
                Iterator<PANDoc> iter = iterable.iterator();
                return new Tuple2<Integer,Iterable<PANDoc>>(IteratorUtils.toList(iter).size(),iterable);
            }
        });*/

        //JavaPairRDD<Integer, Iterable<PANDoc>> sorted = mapped.sortByKey(false);
        return filtered.flatMap(new FlatMapFunction<Tuple2<Double,Iterable<PANDoc>>, PANDoc>() {
            @Override
            public Iterable<PANDoc> call(Tuple2<Double, Iterable<PANDoc>> integerIterableTuple2) throws Exception {
                return integerIterableTuple2._2();
            }
        });



    }

    private void printSorted(JavaPairRDD<Integer, Iterable<PANDoc>> sorted){
        Map<Integer, Iterable<PANDoc>> map = sorted.collectAsMap();
        NavigableMap<Integer, Iterable<PANDoc>> sortedMap = (new TreeMap<>(map)).descendingMap();

        Iterator<Integer> sizeIter = sortedMap.keySet().iterator();
        while(sizeIter.hasNext()){
            Integer key = sizeIter.next();
            Iterator<PANDoc> iter = sortedMap.get(key).iterator();
            PANDoc doc = iter.next();
            System.out.println("Label: "+doc.getAuthorid().replaceAll("\\n","")+" Size:"+key);
        }
    }



    private JavaRDD<PANDoc> panRDD(JavaPairRDD<String, String> docRDD){
        JavaRDD<PAN> panRDD = docRDD.flatMap(new FlatMapFunction<Tuple2<String, String>, PAN>() {
            @Override
            public Iterable<PAN> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String filename = stringStringTuple2._1();
                String text = stringStringTuple2._2();

                return XMLParser.parsePAN(filename, text);
            }
        });

        JavaRDD<PAN> bothRDD = panRDD.filter(new Function<PAN, Boolean>() {
            @Override
            public Boolean call(PAN pan) throws Exception {
                return pan.knownAuthorText();
            }
        });

        final JavaPairRDD<String, String> docAuthorRDD = panRDD.filter(new Function<PAN, Boolean>() {
            @Override
            public Boolean call(PAN pan) throws Exception {
                return pan.authorDocPair()  || pan.knownAuthorText();
            }
        }).mapToPair(new PairFunction<PAN, String, String>() {
            @Override
            public Tuple2<String, String> call(PAN pan) throws Exception {
                return new Tuple2<String, String>(pan.getDocid(),pan.getAuthor());
            }
        });

        final Map<String,String> docAuthorMap = docAuthorRDD.collectAsMap();

        JavaRDD<PAN> unknownAuthorRDD = panRDD.filter(new Function<PAN, Boolean>() {
            @Override
            public Boolean call(PAN pan) throws Exception {
                return pan.unknownAuthorText();
            }
        });

        //Scan and find unknown authors
        JavaRDD<PAN> knownAuthorRDD = unknownAuthorRDD.map(new Function<PAN, PAN>() {
            @Override
            public PAN call(PAN v1) throws Exception {
                if (v1.getAuthor() == null && docAuthorMap.get(v1.getDocid()) == null) {
                    return new PAN(v1.getDocid(), v1.getAuthor(), v1.getText());
                } else if (docAuthorMap.containsKey(v1.getDocid())) {
                    return new PAN(v1.getDocid(), docAuthorMap.get(v1.getDocid()), v1.getText());
                } else {
                    return v1;
                }
            }
        }).filter(new Function<PAN, Boolean>() {
            @Override
            public Boolean call(PAN v1) throws Exception {
                return v1.getAuthor()!=null;
            }
        });

        JavaRDD<PAN> allRDD = bothRDD.union(knownAuthorRDD);
        //Label them by double
        final List<String> authors = allRDD.map(new Function<PAN, String>() {
            @Override
            public String call(PAN pan) throws Exception {

                return pan.getAuthor();
            }
        }).distinct().collect();

        return allRDD.map(new Function<PAN, PANDoc>() {
            @Override
            public PANDoc call(PAN pan) throws Exception {
                String author = pan.getAuthor();
                String text = pan.getText();
                String docid = pan.getDocid();
                double label = authors.indexOf(author);
                return new PANDoc(docid,author,label,text);
            }
        });
    }


    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Reuters Processing">

    /**
     * Group documents by topics
     * 1. generate Reuters Documents
     * 2. Count topics
     *
     * @param sparkContext
     * @return
     */
    public JavaRDD<ReutersDoc> reutersRDD(JavaSparkContext sparkContext, PrintBuffer buffer, int topNtopics) {

        final SentenceML sentenceDetectorML = new SentenceML();

        //Load files and parse documents
        JavaPairRDD<String, String> filesRDD = loadXML(sparkContext, new String[]{REUTERSDIRECTORY});
        JavaRDD<Reuters> reutersRDD = filesRDD.flatMap(new FlatMapFunction<Tuple2<String, String>, Reuters>() {
            @Override
            public Iterable<Reuters> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String filename = stringStringTuple2._1();
                String text = stringStringTuple2._2();
                return XMLParser.parseReuters(filename, text);
            }
        });

        //JavaRDD<Reuters> documentRDD = reutersRDD.values();

        //Flat Map reuters documents by topic
        JavaRDD<ReutersDoc> docRDD = reutersRDD.flatMap(new FlatMapFunction<Reuters, ReutersDoc>() {
            @Override
            public Iterable<ReutersDoc> call(Reuters reuters) throws Exception {
                List<ReutersDoc> reutersList = new ArrayList<>();
                for (String topic : reuters.getTopicList()) {
                    String text = reuters.getText();
                    if (text != null) {
                        String[] sentences = sentenceDetectorML.fit(text);
                        ReutersDoc reutersDoc = new ReutersDoc(text, topic);
                        reutersDoc.setSentences(Arrays.asList(sentences));
                        reutersList.add(reutersDoc);
                    }
                }
                return reutersList;
            }
        });

        JavaPairRDD<String, Iterable<ReutersDoc>> pairRDD = docRDD.groupBy(new Function<ReutersDoc, String>() {
            @Override
            public String call(ReutersDoc v1) throws Exception {
                return v1.getTopic();
            }
        });

        JavaRDD<Tuple2<String, Long>> topicCounts = pairRDD.map(new Function<Tuple2<String, Iterable<ReutersDoc>>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<ReutersDoc>> v1) throws Exception {
                long size = (long) IteratorUtils.toList(v1._2().iterator()).size();
                return new Tuple2<String, Long>(v1._1(), size);
            }
        });

        List<Tuple2<String, Long>> top10Count = topicCounts.top(topNtopics, new TupleSizeComparator());
        HashMap<String, Double> top10Map = new HashMap<>();
        double labelId = 0.0d;
        for (Tuple2<String, Long> tuple : top10Count) {
            top10Map.put(tuple._1(), labelId);
            labelId++;
            buffer.addLine(tuple._1() + "->" + labelId + "->" + tuple._2());
        }

        List<ReutersDoc> documentList = new ArrayList<>();
        for (ReutersDoc reutersDoc : docRDD.collect()) {
            if (top10Map.containsKey(reutersDoc.getTopic()) && !documentList.contains(reutersDoc)) {
                double myLabelid = top10Map.get(reutersDoc.getTopic());
                reutersDoc.label(myLabelid);
                documentList.add(reutersDoc);
            }
        }

        return sparkContext.parallelize(documentList);

    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////




    public static SparkConf initLocal() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("log4j.rootCategory").setLevel(Level.OFF);

        return new SparkConf()
                .setAppName("AuthorIdentification Processing")
                .setMaster("local[6]")
                .set("spark.executor.memory", "20g")
                .set("log4j.rootCategory", Level.OFF.getName());
    }

    public static SparkConf initCluster() {
        Logger.getLogger("org").setLevel(Level.INFO);
        Logger.getLogger("akka").setLevel(Level.INFO);
        String sparkHome = "/home/wolf/Documents/apps/spark-1.5.1-bin-hadoop2.6/";

        return new SparkConf()
                .setAppName("RDDProcessing")
                .set("spark.executor.memory", "4g")
                .set("spark.rdd.compress", "true")

                /*.set("spark.storage.memoryFraction", "1")
                .set("spark.core.connection.ack.wait.timeout", "300")
                .set("spark.akka.frameSize", "20")
                //.set("spark.cores.max","6")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "logs/")
                .set("spark.executor.memory","512m")
                */
                .setJars(new String[]{"out/AuthorIdentification.jar"})
                .setSparkHome(sparkHome)
                .setMaster("spark://192.168.1.3:7077")
                        //.setMaster("local[24]")
                .set("log4j.rootCategory", "INFO");

    }

    public static void main(String[] args) {

        SparkConf conf = initLocal();
        JavaSparkContext sc = new JavaSparkContext(conf);
        RDDProcessing processing = new RDDProcessing();


        /*
        PrintBuffer buffer = new PrintBuffer();
        processing.panRDDLargeSave(sc);
        */

        int debug = 0;
    }

}
