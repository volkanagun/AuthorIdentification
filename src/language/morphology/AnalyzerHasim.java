/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package language.morphology;


import scala.collection.Seq;
import util.TextFile;
import util.TextPattern;
import util.TextReplacer;


import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author wolf
 */
public class AnalyzerHasim implements Serializable {
    // [['çabukluk'], ['çabukluk[Noun]+[A3sg]+[Pnon]+[Nom]', 16.4697265625], ['çabuk[Adj]-lHk[Noun+Ness]+[A3sg]+[Pnon]+[Nom]', 14.72265625]]

    String[] replaces = new String[]{"\\\\xc4\\\\xb1", "ı", "\\\\xc4\\\\xb0", "İ",
            "\\\\xc4\\\\x9f", "ğ", "\\\\xc4\\\\x9e", "Ğ", "\\\\xc3\\\\xbc", "ü",
            "\\\\xc3\\\\x9c", "Ü", "\\\\xc5\\\\x9f", "ş", "\\\\xc5\\\\x9e", "Ş",
            "\\\\xc3\\\\xb6", "ö", "\\\\xc3\\\\x96", "Ö", "\\\\xc3\\\\xa7", "ç",
            "\\\\xc3\\\\x87", "Ç",
            "\\\\xc3\\\\xa2", "a",
            "\\\\xc3\\\\xae","î",
            "\\\\xc3\\\\x8e","Î",
            "\\\\xc3\\\\xa2","â",
            "\\\\xc3\\\\xbb","û",
            "Ã¼", "ü",
            "Å\u009F", "ş",
            "Ã§", "ç",
            "Ä±", "ı",
            "Ä\u009F", "ğ",
            "Ã¶", "ö",
            "Ã\u0096", "Ö",
            "Å\u009E", "Ş",
            "Ä°", "İ"
    };

    private TextReplacer replacer = new TextReplacer(replaces);
    private static String parseCMD = "parse.py";
    private static String disambCMD = "md-input.pl";

    public static String regexLevelOne = "((\\[|\\s)\\[(.*?)\\](\\,|\\]))";
    public static String regexLevelTwo = "(\\])(\\+|\\-|'\\,)";
    public int count = 0;
    private File folderParse = new File("/home/wolf/Documents/java-projects/MorphDisambiguator/morphology/exec/MP-1.0-Linux64/");
    private File folderDisamb = new File("/home/wolf/Documents/java-projects/MorphDisambiguator/morphology/exec/MD-Release/");


    public AnalyzerHasim() {
    }

    private String toDisambiguate(List<MorphResult> morphResults){
        String sentence = "";
        for (MorphResult morphResult : morphResults) {
            String lineString = morphResult.toLineString();
            lineString = lineString.replaceAll("\\\"","\\\\\"");
            sentence += lineString + "\t";
        }
        return sentence;
    }

    public void disambiguate(String[] tokens, List<MorphResult> resultList) {
        List<MorphResult> morphResults = parse(tokens);
        normalize(morphResults);
        String sentence = toDisambiguate(morphResults);


        StringBuffer buffer = new StringBuffer();
        sentence = sentence.replaceAll("'", "\\\\'");
        String command = "./" + disambCMD + " \"" + sentence + "\"";
        CommandProcessor processor = new CommandProcessor(command, folderDisamb.getAbsoluteFile(), replacer, buffer);
        processor.start();

        while (processor.isAlive()) {

        }

        String result = buffer.toString();
        obtainDisambiguationResult(result, resultList);

    }

    public Seq<MorphResult> disambiguateScala(String[] tokens) {

        List<MorphResult> disambiguateList = new ArrayList<>();
        disambiguate(tokens, disambiguateList);
        return scala.collection.JavaConversions.collectionAsScalaIterable(disambiguateList).toSeq();

    }


    public void parse(String[] tokens, List<MorphResult> resultList){
        String sentence = toSentence(tokens);
        sentence = sentence.replaceAll("'", "\\\\'");
        sentence = sentence.replaceAll("\"", "\\\\\"");
        sentence = sentence.replaceAll("\\(", "\\\\(");
        sentence = sentence.replaceAll("\\)", "\\\\)");

        //sentence = sentence.replaceAll("\\s", "\\\\ ");

        StringBuffer buffer = new StringBuffer();

        String command = "./" + parseCMD+" "+sentence;
        CommandProcessor processor = new CommandProcessor(command, folderParse.getAbsoluteFile(), replacer, buffer);
        processor.start();

        while (processor.isAlive()) {
        }

        obtainResult(buffer.toString(),tokens, resultList);
    }

    public List<MorphResult> parse(String[] tokens){
        List<MorphResult> resultList = new ArrayList<>();
        parse(tokens,resultList);
        return resultList;
    }

    public Seq<MorphResult> parseScala(String[] tokens){
        List<MorphResult> resultList = new ArrayList<>();
        parse(tokens,resultList);
        return scala.collection.JavaConversions.collectionAsScalaIterable(resultList).toSeq();
    }



    public MorphResult parseToken(String token) {

        MorphResult morphResult = new MorphResult(token);
        StringBuffer buffer = new StringBuffer();


        token = token.replaceAll("'", "\\\\'");
        token = token.replaceAll("\\s", "\\\\ ");
        String[] command = new String[]{"./" + parseCMD, "" + token + ""};
        CommandProcessor processor = new CommandProcessor(command, folderParse.getAbsoluteFile(), replacer, buffer);
        processor.start();

        while (processor.isAlive()) {
        }

        obtainResult(buffer.toString(), morphResult);
        return morphResult;
    }


    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="No use">
    public void extractDismabiguationToText(String inputFile, String outputFile) {
        TextFile inFile = new TextFile(inputFile);
        TextFile outFile = new TextFile(outputFile);

        inFile.openBufferRead();
        outFile.openBufferWrite();

        String line;
        int cond = 0;
        while ((line = inFile.readNextLine()) != null) {

            if (line.startsWith("<S>")) {
                cond = 1;
                outFile.writeNextLine("<start>");

            } else if (line.startsWith("</S>")) {
                cond = 0;
                outFile.writeNextLine("<end>");

            } else if (cond == 1) {
                int indexFirst = line.indexOf(" ");
                int indexSecond = line.indexOf(" ", indexFirst + 1);
                String word = line.substring(0, indexFirst);
                String posser = "";
                String der = "DB";
                if (indexSecond == -1) {
                    posser = line.substring(indexFirst + 1);

                } else {
                    posser = line.substring(indexFirst, indexSecond);
                }

                String pos = extractPOS(posser);

                outFile.writeNextLine(word + " " + pos);


            }


        }

        inFile.closeBufferRead();
        outFile.closeBufferWrite();


    }
    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    private void normalize(List<MorphResult> morphResults){
        for(MorphResult morphResult:morphResults){
            normalize(morphResult);
        }
    }

    private void normalize(MorphResult morphResult){
        if(morphResult.getNone()){
            morphResult.addMorphUnit(new MorphUnit("Punc",morphResult.getToken()));
        }
    }

    private static String toSentence(String[] tokens){
        String result = "";
        for(String token:tokens){
            result+=" "+token;
        }

        return result.trim();
    }

    private static String extractPOS(String posser) {

        List<String> resultList = new LinkedList<>();

        TextPattern.obtainPatterns("(Noun|Verb|Adj|Adverb|Det|Pron|Punc|Conj|Postp|Pron|Num|Dup|Interj|Prop)", 1, false, posser, resultList);

        if (resultList.isEmpty()) {
            return "Unknown";
        } else {
            String val = resultList.get(resultList.size() - 1);
            if (val.equals("")) {
                return "Unknown";
            } else {
                return val;
            }
        }


    }

    protected static void obtainDisambiguationResult(String all, List<MorphResult> list) {
        String[] lines = all.split("\n");
        for (String line : lines) {
            MorphResult morphResult = obtainDisambiguationResult(line);
            if (morphResult != null) {
                list.add(morphResult);
            }
        }
    }

    protected static void obtainResult(String all, String[] tokens, List<MorphResult> list) {
        try {
            String[] lines = all.split("\n");
            if(lines.length==tokens.length) {
                for (int i = 0; i < lines.length; i++) {
                    String line = lines[i];
                    String token = tokens[i];
                    MorphResult morphResult = new MorphResult(token);
                    obtainResult(line, morphResult);
                    if (morphResult != null) {
                        list.add(morphResult);
                    }
                }
            }
        }
        catch(ArrayIndexOutOfBoundsException ex){
            ex.printStackTrace();
        }
    }


    protected static MorphResult obtainDisambiguationResult(String line) {
        String[] morphBest = line.split("\t");


        if (morphBest.length == 1) return null;
        else {
            MorphResult result = new MorphResult(morphBest[0]);
            String pos = extractPOS(morphBest[1]);
            String[] tags = morphBest[1].split("\\+");
            MorphUnit morphUnit = new MorphUnit();
            morphUnit.setStem(tags[0]);
            morphUnit.setPrimaryPos(pos);
            for (int i = 1; i < tags.length; i++) {
                morphUnit.addTag(tags[i], tags[i]);
            }

            result.addMorphUnit(morphUnit);
            return result;
        }
    }

    protected static void obtainResult(String resultLine, MorphResult morphResult) {
        //resultLine = resultLine;//.replaceAll("\'", "");
        Pattern patternOne = Pattern.compile(regexLevelOne);
        Matcher matcherOne = patternOne.matcher(resultLine);
        int count = 0;
        while (matcherOne.find()) {
            MorphUnit morphunit = new MorphUnit();
            String group = matcherOne.group(3);
            String[] groupSplit = group.split(regexLevelTwo);

            if (group.contains("unknown")) {
                morphResult.addMorphUnit(new MorphUnit());
                break;
            } else if (count > 0) {
                String[] morphySplit = groupSplit[0].split("\\[");
                String primaryPos = "Punc", stem = "'";
                if (!groupSplit[0].startsWith("\"'[")) {

                    primaryPos = morphySplit[1].replaceAll("\'", "");
                    stem = morphySplit[0].replaceAll("\'", "");
                }


                morphunit.setPrimaryPos(primaryPos);
                morphunit.setStem(stem);


                //Obtain full tag sequence not start from 1
                for (int i = 0; i < groupSplit.length; i++) {
                    String morphy = groupSplit[i].replaceAll("\'", "").trim();


                    if (!morphy.matches("(\\d+\\.\\d+)")) {
                        morphySplit = morphy.split("\\[");
                        String tag = morphySplit[1].replaceAll("\'", "");
                        String val = morphySplit[0].replaceAll("\'", "");
                        morphunit.addTag(val, tag);


                    } else {
                        morphunit.setProb(Double.parseDouble(morphy));
                    }

                }

                morphResult.addMorphUnit(morphunit);
            }

            count++;
        }

        morphResult.setLabel(morphResult.getFirstResult());

    }

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Static Functions">
    public static void main(String[] args) {
        List<MorphResult> resultList = new ArrayList<>();
        AnalyzerHasim hasim = new AnalyzerHasim();
        String sentence = "Fehmi Koru - Yeni Şafak : [ ... ] MGK toplantılarının gizliliği sebebiyle , üyelerin içeride konuşulanları aktarması , gazetelerin de bunları yayınlaması \" suç \" teşkil eder ama , yapılan yayın ne kadar hayati bir konuda olursa olsun herhalde \" vatana ihanet \" sayılmaz .";

        //hasim.parse(sentence.split("\\s"), resultList);
        hasim.disambiguate(sentence.split("\\s"), resultList);

        System.out.println(resultList.toString());
        int debug = 0;
    }
    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
}
