package models.opennlp;

import opennlp.tools.sentdetect.*;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.TrainingParameters;
import scala.collection.JavaConversions;


import scala.collection.immutable.Seq;
import scala.collection.mutable.StringBuilder;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wolf on 13.08.2015.
 */
public class SentenceDetectorML implements Serializable{

    private String modelFilename = "resources/training/sentences.bin";
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

        ObjectStream<String> lineStream = new PlainTextByLineStream(new FileInputStream(filename), charset);
        ObjectStream<SentenceSample> sampleStream = new SentenceSampleStream(lineStream);
        SentenceDetectorFactory sentenceDetectorFactory = new SentenceDetectorFactory();


        SentenceModel model;

        try {
            model = SentenceDetectorME.train("tr", sampleStream, true, null, TrainingParameters.defaultParams());
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
