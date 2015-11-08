package opennlp.models;

import opennlp.tools.sentdetect.*;
import opennlp.tools.util.*;


import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by wolf on 13.08.2015.
 */
public class SentenceDetectorML implements Serializable{

    private String modelFilename = "/home/wolf/Documents/java-projects/AuthorIdentification/resources/training/sentences.bin";
    private SentenceDetectorME sentenceDetector;

    public SentenceDetectorML() {
        init();
    }

    private void init() {
        try {
            FileInputStream inputStream = new FileInputStream(modelFilename);
            SentenceModel model = new SentenceModel(inputStream);
            sentenceDetector = new SentenceDetectorME(model);

        } catch (IOException ex) {

        }
    }

    public String[] fit(String text) {
        return sentenceDetector.sentDetect(text);

    }



    public void train(final String filename) throws IOException {
        Charset charset = Charset.forName("UTF-8");
        //InputStreamFactory inputStream = new MarkableFileInputStreamFactory(new File(filename));
        ObjectStream<String> lineStream = new PlainTextByLineStream(new FileInputStream(filename), charset);
        ObjectStream<SentenceSample> sampleStream = new SentenceSampleStream(lineStream);
        SentenceDetectorFactory sentenceDetectorFactory = new SentenceDetectorFactory();


        SentenceModel model;

        try {
            SentenceDetectorFactory detectorFactory = new SentenceDetectorFactory();
            model = SentenceDetectorME.train("tr", sampleStream, detectorFactory, TrainingParameters.defaultParams());
        } finally {
            sampleStream.close();
        }

        OutputStream modelOut = null;
        try {
            modelOut = new BufferedOutputStream(new FileOutputStream(modelFilename));
            model.serialize(modelOut);
        } finally {
            if (modelOut != null)
                modelOut.close();
        }
    }

    public static void main(String[] args) throws IOException {

        String trainFilename = "resources/training/sentences-all.txt";
        SentenceDetectorML trainer = new SentenceDetectorML();
        trainer.train(trainFilename);
    }
}
