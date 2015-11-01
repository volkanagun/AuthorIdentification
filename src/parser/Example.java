package parser;

import main.RuleTokenizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class Example implements Serializable {

    public static String NUMBER = "NUMBER";

    public Regex repeatingPunctuation = new Regex("repeatingPunctuation", "(\\p{P}{2,})", "$1");
    public Regex dotPunctuation = new Regex("dotPunctuation", "(\\.)", "$1");
    public Regex dotCommaPunctuation = new Regex("dotCommaPunctuation", "(\\.|\\,)", "$1");
    public Regex hypPunctuation = new Regex("hypPunctuation", "(\\-)", "$1");
    public Regex numberRegex = new Regex("numberRegex", "(\\d+)", "$1");
    public Regex numberDot = new Regex("numberRegex", "(\\d+([\\.\\,]\\d?)+)", "$1");

    public Regex lowercaseLetter = new Regex("lowercaseLetter", "\\p{Lo}", "$1");
    public Regex uppercaseLetter = new Regex("uppercaseLetter", "\\p{Lu}", "$1");
    public Regex someLetter = new Regex("someLetter", "\\p{L}+", "$1");

    public Regex moneySymbol = new Regex("moneySymbol", "(\\$|ман|BZ$|$b|лв|¥|₡|Kč|£|€|¢|₩|ден|₮|₦|฿|₤)", "$1");
    public Regex abbreviation = new Regex("abbreviation", "(Ph\\.[Dd]\\.|[Dd]r\\.)", "$1");

    public List<Rule> ruleNumberDot = Rule.createRule("numberDot", new Regex[]{numberRegex, dotCommaPunctuation});
    public List<Rule> ruleNumber = Rule.createRule("numberDot", ruleNumberDot, new Regex[]{numberRegex});
    public List<Rule> ruleNumberRec = Rule.createRule("numberDot", ruleNumberDot, ruleNumber);

    public List<Rule> ruleAbbrvDot = Rule.createRule("abbrvDot", new Regex[]{uppercaseLetter, dotPunctuation});
    public List<Rule> ruleAbbrvRec = Rule.createRule("abbrvRec", ruleAbbrvDot, ruleAbbrvDot);


    //Tokenize punctuations
    //Numbers, Abbreviations
    //Split by space
    private Engine engine;

    public Example() {
        this.engine = new Engine();

        //Rules
        engine.addRule(ruleNumberDot);
        engine.addRule(ruleNumber);
        engine.addRule(ruleNumberRec);
        engine.addRule(ruleAbbrvDot);
        engine.addRule(ruleAbbrvRec);

    }

    public String[] tokenize(String text) {
        List<Match> matches = this.engine.parse(text);
        List<String> matchList = new ArrayList<>();
        for (Match match : matches) {
            matchList.add(match.matchString(text) + "_" + match.getLabel() + "_" + match.getStart() + "/" + match.getEnd());
        }

        return matchList.toArray(new String[0]);
    }

    public static void main(String[] args) {
        RuleTokenizer tokenizer = new RuleTokenizer();

        String text = "U.S. 1000.000.000,00$ PhD. Ali Korkar icin odeme yaptilar...";
        String[] tokens = tokenizer.tokenize(text);

        for (String token : tokens) {
            System.out.print(" " + token);
        }

    }

    /*private String month(String value){
        if(value.matches("[Oo]cak?")) return "01";
        else if(value.matches("[Şş](ub|ubat)")) return "02";
        else if(value.matches("[Mm]art?")) return "03";
        else if(value.matches("[Nn](is|isan)")) return "04";
        else if(value.matches("[Mm](ay|ayıs)")) return "05";
        else if(value.matches("[Hh](az|aziran)")) return "06";
        else if(value.matches("[Tt](em|emmuz)")) return "07";
        else if(value.matches("[Aa](ğu|ğustos)")) return "08";
        else if(value.matches("[Ee](yl|ylül)")) return "09";
        else if(value.matches("[Ee](km|kim)")) return "10";
        else if(value.matches("[Kk](as|asım)")) return "11";
        else if(value.matches("[Aa](ra|ralık)")) return "12";
        else return value;
    }*/
}
