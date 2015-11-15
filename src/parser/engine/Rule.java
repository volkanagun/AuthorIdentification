package parser.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class Rule implements Serializable {
    private String label;
    private Regex left, right;
    private boolean generated;
    private boolean single;

    public Rule() {
    }

    public Rule(String label, Regex regex, boolean single) {
        this.label = label;
        this.left = regex;
        this.right = Regex.emptyRegex();
        this.single = single;
    }

    public Rule(String label, Regex left, Regex right) {
        this.label = label;
        this.left = left;
        this.right = right;
    }

    public Rule(String label, String left, Regex right) {
        this.label = label;
        this.right = right;
        this.left = new Regex(left, null, null);
    }

    public Rule(String label, String left, String right) {
        this.label = label;
        this.left = new Regex(left, null, null);
        this.right = new Regex(right, null, null);
    }

    public Rule(String label, Regex left, String right) {
        this.label = label;
        this.left = left;
        this.right = new Regex(right, null, null);
    }

    public Regex getLeft() {
        return left;
    }

    public void setLeft(Regex left) {
        this.left = left;
    }

    public Regex getRight() {
        return right;
    }

    public void setRight(Regex right) {
        this.right = right;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isGenerated() {
        return generated;
    }

    public Rule setGenerated(boolean generated) {
        this.generated = generated;
        return this;
    }

    public boolean isSingle() {
        return single;
    }

    public Rule setSingle(boolean single) {
        this.single = single;
        return this;
    }

    @Override
    public String toString() {
        return label + "=" + left.getLabel() + " " + right.getLabel();
    }

    public static List<Rule> createRule(String label, List<Rule> ruleLeftList, List<Rule> ruleRightList) {
        List<Rule> ruleList = new ArrayList<>();
        Rule ruleLeft = ruleLeftList.get(ruleLeftList.size() - 1);
        Rule ruleRight = ruleLeftList.get(ruleRightList.size() - 1);

        Rule rule = new Rule(label, ruleLeft.getLabel(), ruleRight.getLabel());
        rule.setGenerated(false);
        ruleList.add(rule);
        return ruleList;
    }

    public static List<Rule> createRule(String label, Regex[] regexes, Rule rule) {
        List<Rule> ruleList = new ArrayList<>();
        Rule ruleRegex = createRule(label, regexes, ruleList, 0, true);
        Rule ruleNew = new Rule(label, ruleRegex.getLabel(), rule.getLabel());
        ruleNew.setGenerated(false);
        ruleList.add(ruleRegex);
        ruleList.add(ruleNew);

        return ruleList;
    }

    public static List<Rule> createRule(String label, List<Rule> rules, Regex[] regexes) {
        List<Rule> ruleList = new ArrayList<>();
        Rule rule = rules.get(rules.size() - 1);
        if (regexes.length >= 2) {
            int index = 0;
            Rule ruleRegex = createRule(label + index, regexes, ruleList, index, true);
            Rule ruleNew = new Rule(label, rule.getLabel(), ruleRegex.getLabel());
            ruleNew.setGenerated(false);
            ruleList.add(ruleRegex);
            ruleList.add(ruleNew);
        } else {
            Regex ruleRegex = regexes[regexes.length - 1];
            Rule ruleNew = new Rule(label, rule.getLabel(), ruleRegex);
            ruleNew.setGenerated(false);
            ruleList.add(ruleNew);
        }
        return ruleList;
    }

    public static List<Rule> createRule(String label, Regex[] regexes) {
        List<Rule> ruleList = new ArrayList<>();
        if (regexes.length >= 2) {
            Rule rule = createRule(label, regexes, ruleList, 0, false);
            ruleList.add(rule);
        }
        return ruleList;
    }

    public static Rule createRule(Regex regex) {
        Rule rule = new Rule(regex.getLabel(), regex.setRule(true), true);
        return rule;
    }

    private static Rule createRule(String label, Regex[] regexes, List<Rule> ruleList, int index, boolean generated) {
        if (regexes.length - index == 2) {
            Rule rule = new Rule(label, regexes[index], regexes[index + 1]);
            rule.setGenerated(generated);
            return rule;
        } else if (regexes.length - index > 2) {

            Regex regex = regexes[index];
            Rule rule = createRule(label + index, regexes, ruleList, index + 1, true);
            Rule newRule = new Rule(label, regex, rule.getLabel());

            rule.setGenerated(true);
            newRule.setGenerated(false);
            ruleList.add(rule);
            return newRule;
        }

        return null;
    }

}
