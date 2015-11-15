package parser.examples;

import parser.engine.Engine;
import parser.engine.Match;
import parser.engine.Regex;
import parser.engine.Rule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class RuleTokenizer implements Serializable {

    public static String NUMBER = "NUMBER";

    public Regex repeatingPunctuation = new Regex("repeatingPunctuation", "(\\p{P}{2,})", "$1");
    public Regex dotPunctuation = new Regex("dotPunctuation", "(\\.)", "$1");
    public Regex dotCommaPunctuation = new Regex("dotCommaPunctuation", "(\\.|\\,)", "$1");
    public Regex hypPunctuation = new Regex("hypPunctuation", "(\\-)", "$1");
    public Regex numberRegex = new Regex("numberRegex", "(\\d+)", "$1");
    public Regex numberDot = new Regex("numberRegex", "(\\d+([\\.\\,]\\d?)+)", "$1");

    public Regex lowercaseLetter = new Regex("lowercaseLetter", "(\\p{Lo})", "$1");
    public Regex uppercaseLetter = new Regex("uppercaseLetter", "(\\p{Lu})", "$1");
    public Regex someLetter = new Regex("someLetter", "(\\p{L}+)", "$1");

    public Regex moneySymbol = new Regex("moneySymbol", "(\\$|ман|BZ$|$b|лв|¥|₡|Kč|£|€|¢|₩|ден|₮|₦|฿|₤)", "$1");
    public Regex abbreviation = new Regex("abbreviation", "(Ph[Dd]\\.|[Dd]r\\.|Mr\\.|Mrs\\.|Ms.)", "$1");

    public Rule letters = Rule.createRule(new Regex("LETTERS","(\\p{L}+)","$1"));
    public Rule repeatingPunc = Rule.createRule(repeatingPunctuation);
    public List<Rule> abbreviationRule = Rule.createRule("abbreviationRule",new Regex[]{uppercaseLetter,dotPunctuation});
    public List<Rule> abbreviationSemi = Rule.createRule("abbreviationRule",abbreviationRule,abbreviationRule);


    //Tokenize punctuations
    //Numbers, Abbreviations
    //Split by space
    private Engine engine;

    public RuleTokenizer() {
        this.engine = new Engine();
        this.engine.addRule(letters);
        this.engine.addRule(repeatingPunc);
        this.engine.addRule(abbreviationRule);
        this.engine.addRule(abbreviationSemi);


        //Rules
        /*engine.addRule(ruleNumberDot);
        engine.addRule(ruleNumber);
        engine.addRule(ruleNumberRec);
        engine.addRule(ruleAbbrvDot);
        engine.addRule(ruleAbbrvRec);*/

    }

    public String[] tokenizeDebug(String text) {
        List<Match> matches = this.engine.parse(text);
        List<String> matchList = new ArrayList<>();
        for (Match match : matches) {
            matchList.add(match.matchString(text) + "_" + match.getLabel() + "_" + match.getStart() + "/" + match.getEnd());
        }

        return matchList.toArray(new String[0]);
    }

    public static void main(String[] args) {
        RuleTokenizer tokenizer = new RuleTokenizer();

        String text = "U.S. 1000.000.000,00$ PhD... Ali Korkar icin odeme yaptilar...";
        String[] tokens = tokenizer.tokenizeDebug(text);

        for (String token : tokens) {
            System.out.print(" " + token);
        }
        int debug = 0;
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
