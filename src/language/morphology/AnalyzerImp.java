package language.morphology;

import scala.collection.Seq;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wolf on 02.01.2016.
 */
public interface AnalyzerImp extends Serializable
{
    AnalyzerImp init();
    String[] tokenize(String sentence);

    MorphResult analyze(String token);
    MorphLight analyzeAsLight(String token);

    String stem(String token);
    String stemSnowball(String token);
    Seq<String> stemBy(String[] tokens, String posFilter);


    Seq<MorphResult> analyzeAsSeq(String[] tokens);
    Seq<MorphLight> analyzeAsLightSeq(String[] tokens);
    Seq<MorphResult> disambiguateAsSeq(String[] tokens);
    Seq<MorphLight> disambiguateAsLightSeq(String[] tokens);




    List<String> unknown(String sentence);
    List<String> unknown(String[] tokens);
    int windowSize();
}
