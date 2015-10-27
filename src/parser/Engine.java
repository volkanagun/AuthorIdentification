package parser;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wolf on 26.06.2015.
 */
public class Engine implements Serializable {
    private List<Regex> regexList;
    private Table<String, String, List<Rule>> ruleTable;

    public Engine() {
        regexList = new ArrayList<>();
        ruleTable = HashBasedTable.create();
    }

    public void addRule(List<Rule> rules){
        for(Rule rule:rules){
            addRule(rule);
        }
    }

    public void addRegex(Regex regex){
        if (!regexList.contains(regex) && regex.isRegex()) regexList.add(regex);

    }

    public Engine addRule(Rule rule) {
        if(!rule.isSingle()) {
            Regex leftRegex = rule.getLeft();
            Regex rightRegex = rule.getRight();

            addRegex(leftRegex);
            addRegex(rightRegex);

            String left = leftRegex.getLabel();
            String right = rightRegex.getLabel();
            if (ruleTable.contains(left, right)) ruleTable.get(left, right).add(rule);
            else {
                List<Rule> ruleList = new ArrayList<>();
                ruleList.add(rule);
                ruleTable.put(left, right, ruleList);
            }
        }
        else{
            Regex regex = rule.getLeft();
            addRegex(regex);
        }

        return this;
    }

    public List<Match> scan(String text){

        List<Match> matchList = new ArrayList<>();

        for(Regex regex:regexList)
        {
            String rg = regex.getRegex();
            String lb = regex.getLabel();
            boolean rule = regex.isRule();
            Pattern pt = regex.getPattern();
            Matcher mt = pt.matcher(text);

            while(mt.find()){
                int start = mt.start();
                int end = mt.end();
                String value = pt.matcher(text.substring(start,end)).replaceAll(regex.getReplace());
                Match match = new Match(lb,value, start, end);
                match.setRegex(!rule);
                matchList.add(match);
            }
        }

        return Match.regroup(matchList);
    }

    public List<Match> parse(String text){
        List<Match> matchList = scan(text);
        Chart chart = new Chart(matchList);
        int size = matchList.size();
        for (int j = 1; j < size; j++) {

            for (int i = 0; i < size - j; i++) {
                Cell cell = chart.getCell(i,j);

                for (int k = 0; k < j; k++) {
                    int rwLeft = i;
                    int clLeft = k;
                    int rwRight = rwLeft + clLeft + 1;
                    int clRight = j - k - 1;

                    Cell leftCell = chart.getCell(rwLeft, clLeft);
                    Cell rightCell = chart.getCell(rwRight, clRight);

                    List<Match> leftMatches = leftCell.getMatches();
                    List<Match> rightMatches = rightCell.getMatches();

                    for(Match leftMatch:leftMatches){
                        String leftLabel = leftMatch.getLabel();
                        int start = leftMatch.getStart();
                        for(Match rightMatch:rightMatches){
                            String rightLabel = rightMatch.getLabel();
                            int end = rightMatch.getEnd();
                            List<Rule> rules = ruleTable.get(leftLabel, rightLabel);
                            if(rules!=null)
                                for(Rule rule:rules){
                                    String label = rule.getLabel();
                                    Match match = new Match(label, start,end);
                                    match.addSubMatch(leftMatch);
                                    match.addSubMatch(rightMatch);
                                    match.setGenerated(rule.isGenerated());
                                    cell.addMatch(match);
                                }
                        }
                    }
                }
            }
        }

        //chart.printChart();
        return chart.chartResult();
    }


}
