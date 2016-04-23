package language.parser.examples;

import language.parser.engine.Engine;
import language.parser.engine.Match;
import language.parser.engine.Regex;
import language.parser.engine.Rule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 12.07.2015.
 */
public class RuleChunker implements Serializable{
    private Engine engine = new Engine();
    private static Regex stoppingPunctuation = new Regex("stoppingPunctuation", "([\\.\\!\\?]+)", "$1");
    private static Regex abbreviation = new Regex("abbreviation", "((\\p{Lu}\\.)+)", "$1");
    private static Regex letters = new Regex("letters", "(\\p{L}+)", "$1");
    private static Regex link = new Regex("link", "((https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])", "$1");
    private static Regex quoted = new Regex("quoted", "('(\\p{Lo}+))", "$2");


    public static Regex phone = new Regex("phone", "((\\(\\+?\\d{2}\\))?[\\-\\s\\.]?\\(?(\\d{3})\\)?[\\.\\-\\s](\\d{3})[\\-\\.\\s](\\d{4}))", "$2$3$4$5");


    public RuleChunker() {
    }

    public void createRules() {
        engine.addRule(Rule.createRule(phone))
                .addRule(Rule.createRule(link))
                .addRule(Rule.createRule(abbreviation))
                .addRule(Rule.createRule(quoted))
                .addRule(Rule.createRule(letters));

    }

    public String[] tokenize(String text) {
        List<Match> matches = this.engine.parse(text);
        List<String> matchList = new ArrayList<>();
        for (Match match : matches) {
            if (match.isValue()) {
                matchList.add(match.getValue() + "_" + match.getLabel() + "_" + match.getStart() + "/" + match.getEnd());
            } else {
                matchList.add(match.matchString(text) + "_" + match.getLabel() + "_" + match.getStart() + "/" + match.getEnd());
            }
        }

        return matchList.toArray(new String[0]);
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public static void main(String[] args) {
        RuleChunker ruleChunker = new RuleChunker();
        ruleChunker.createRules();

        String text = "(284) 235 7580...U.S.A'den geldi";
        String[] tokens = ruleChunker.tokenize(text);

        for (String token : tokens) {
            System.out.print(" " + token);
        }
        int debug = 0;
    }
}
