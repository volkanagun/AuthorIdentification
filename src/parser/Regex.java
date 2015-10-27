package parser;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Created by wolf on 26.06.2015.
 */
public class Regex implements Serializable{

    public static String EMPTYLABEL = "!";

    private String regex;
    private String label;
    private String replace;

    private Pattern pattern;
    private boolean rule;

    public Regex(String label, String regex, String replace)
    {
        this.regex = regex;
        this.label = label;
        this.replace = replace;

        if(this.regex!=null) pattern = Pattern.compile(regex);
    }

    public Pattern getPattern() {
        return pattern;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getReplace() {
        return replace;
    }

    public void setReplace(String replace) {
        this.replace = replace;
    }

    public boolean isRule() {
        return rule;
    }

    public Regex setRule(boolean rule) {
        this.rule = rule;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Regex regex = (Regex) o;

        return label.equals(regex.label);

    }

    public boolean isRegex(){
        return regex!=null;
    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    @Override
    public String toString() {
        return "Regex{" +
                "regex='" + regex + '\'' +
                ", label='" + label + '\'' +
                ", replace='" + replace + '\'' +
                '}';
    }

    public static Regex emptyRegex(){
        return new Regex(EMPTYLABEL, "", "");
    }


}
