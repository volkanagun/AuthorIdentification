package language.morphology;

import com.hrzafer.reshaturkishstemmer.Resha;
import language.morphology.EmptyTagModel;
import language.morphology.TagModel;
import language.tokenization.MyTokenizer;
import scala.collection.JavaConversions;
import scala.collection.Seq;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 03.01.2016.
 */
public class HasimAnalyzerImp implements AnalyzerImp {
    private MyTokenizer tokenizer;
    private AnalyzerHasim analyzer;
    private Resha stemmer = Resha.Instance;
    private int windowSize;
    private final TagModel tagModel;

    public HasimAnalyzerImp(TagModel tagModel) {
        this.windowSize = 2;
        this.tagModel = tagModel;
        tokenizer = new MyTokenizer();
        analyzer = new AnalyzerHasim();
    }

    public HasimAnalyzerImp(TagModel tagModel, int windowSize) {
        this.tagModel = tagModel;
        this.windowSize = windowSize;
        tokenizer = new MyTokenizer();
        analyzer = new AnalyzerHasim();
    }


    private String toSentence(String[] tokens) {
        String sentence = "";

        for (String token : tokens) {
            sentence += " " + token;
        }

        return sentence.trim();
    }

    @Override
    public HasimAnalyzerImp init() {
        return this;
    }

    @Override
    public int windowSize() {
        return windowSize;
    }

    @Override
    public String[] tokenize(String sentence) {
        return tokenizer.tokenizeAsArray(sentence);
    }

    @Override
    public MorphResult analyze(String token){
        return analyzer.parseToken(token);
    }

    @Override
    public String stem(String token){
        return stemmer.stem(token);
    }

    public List<MorphResult> analyze(String[] tokens) {
        List<MorphResult> resultList = new ArrayList<>();
        analyzer.parse(tokens, resultList);
        mapResult(resultList);
        return resultList;
    }

    public Seq<MorphResult> analyzeAsSeq(String[] tokens) {
        List<MorphResult> resultList = analyze(tokens);
        return JavaConversions.asScalaIterable(resultList).toSeq();
    }



    @Override
    public Seq<MorphLight> analyzeAsLightSeq(String[] tokens) {
        List<MorphResult> resultList = analyze(tokens);
        List<MorphLight> lightList = new ArrayList<>();
        for(MorphResult result:resultList){
            MorphLight light = new MorphLight(result.getToken());
            for(MorphUnit units:result.getResultList()){
                light.addAnalysis(units.buildFullTagString());
            }

            lightList.add(light);

        }

        return JavaConversions.asScalaIterable(lightList).toSeq();
    }

    public List<MorphResult> disambiguate(String[] tokens) {
        List<MorphResult> resultList = new ArrayList<>();
        analyzer.disambiguate(tokens, resultList);
        mapResult(resultList);

        return resultList;
    }

    @Override
    public Seq<MorphResult> disambiguateAsSeq(String[] tokens) {
        List<MorphResult> analyzeResults = analyze(tokens);
        List<MorphResult> disambiguatedResults = disambiguate(tokens);
        //Get disambiguated results as labels
        if(analyzeResults.size()==disambiguatedResults.size()) {
            for (int i = 0; i < analyzeResults.size(); i++){
                MorphUnit label = disambiguatedResults.get(i).getFirstResult();
                analyzeResults.get(i).setLabel(label);
            }
            return JavaConversions.asScalaIterable(analyzeResults).toSeq();
        }
        else{
            return JavaConversions.asScalaIterable(new ArrayList<MorphResult>()).toSeq();
        }
    }

    @Override
    public Seq<MorphLight> disambiguateAsLightSeq(String[] tokens) {
        List<MorphResult> analyzeResults = analyze(tokens);
        List<MorphResult> disambiguatedResults = disambiguate(tokens);
        List<MorphLight> lightList = new ArrayList<>();
        if(analyzeResults.size() == disambiguatedResults.size()){
            for(int i=0; i< analyzeResults.size(); i++){
                MorphResult analyzeResult = analyzeResults.get(i);
                MorphUnit analyzeLabel = disambiguatedResults.get(i).getFirstResult();
                MorphLight morphLight = new MorphLight(analyzeResult.getToken());
                for(MorphUnit unit:analyzeResult.getResultList()){
                    morphLight.addAnalysis(unit.buildFullTagString());
                }

                morphLight.setLabel(analyzeLabel.buildFullTagString());
            }

        }

        return JavaConversions.asScalaIterable(lightList).toSeq();
    }

    public List<MorphResult> disambiguateLikely(String[] tokens) {
        List<MorphResult> resultList = analyze(tokens);

        for(MorphResult morphResult:resultList){
            morphResult.clearUnLikely();
        }

        return resultList;
    }

    @Override
    public List<String> unknown(String sentence) {
        return null;
    }

    @Override
    public List<String> unknown(String[] tokens) {
        return null;
    }


    private void mapResult(MorphResult morphResult){
        List<MorphUnit> unitList = morphResult.getResultList();
        for(MorphUnit unit:unitList){

            String ppos = tagModel.mapTag(unit.getPrimaryPos());
            String spos = tagModel.mapTag(unit.getSecondaryPos());

            unit.setPrimaryPos(ppos);
            unit.setSecondaryPos(spos);

            List<String[]> tags = unit.getTags();
            //String[] tagArray = unit.getTagArray();
            for(int i=0; i<tags.size();i++){
                String[] tag = tags.get(i);
                String tg = tag[0];
                tag[0] = tagModel.mapTag(tg);
                tags.set(i, tag);
            }

            unit.setUpdated(true);
        }

    }

    private void mapResult(List<MorphResult> morphResultList){
        for(MorphResult morphResult:morphResultList){
            mapResult(morphResult);
        }
    }

    public static void main(String[] args) {
        HasimAnalyzerImp stemmer = new HasimAnalyzerImp(new EmptyTagModel(),3).init();
        List<MorphResult> result1s = stemmer.analyze(new String[]{"onuncalar", "masalı","odaya","getirdi"});
        //Context context2 = stemmer.parse("Tabii ki oyun kurucular onlar, ama bizler de görüşlerimizi piyasadaki oyuncular olarak söylemek durumundayız.");

        for(MorphResult result:result1s)
        {

            System.out.println(result+"   "+result.minimumDifferenceSet());
        }

       //System.out.println(context2.build().listStringPairs());
    }
}
