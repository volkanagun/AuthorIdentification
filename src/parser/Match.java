package parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class Match implements Serializable, Comparable<Match> {

    private String label,value;
    private int start, end;
    private boolean isExtracted, generated, regex;
    private List<Match> groups;

    public Match(String label, int start, int end) {
        this.label = label;
        this.start = start;
        this.end = end;
        this.groups = new ArrayList<>();
    }


    public Match(String label,String value, int start, int end) {
        this.label = label;
        this.value = value;
        this.start = start;
        this.end = end;
        this.groups = new ArrayList<>();
    }

    public void addSubMatch(Match match){
        this.groups.add(match);
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public List<Match> getGroups() {
        return groups;
    }

    public void clearGroups(){
        groups.clear();
    }

    public boolean contains(Match match){
        if(start<=match.start && end>=match.end) return true;
        else return false;
    }

    public boolean intersect(Match match){
        if(match.start > start && match.start < end && match.end > end) return true;
        else if(start > match.start && start < match.end && end > match.end) return true;
        else return false;
    }

    @Override
    public int compareTo(Match o) {
        if(o.start == start && o.end == end) return 0;
        if(o.start<start) return +1;
        else if(o.start>start) return -1;
        else if(o.start==start && o.end>end) return +1;
        else if(o.start==start && o.end<end) return -1;
        else if(o.start==start && o.end==end) return 0;
        else return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Match match = (Match) o;

        return label.equals(match.label);

    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    public boolean isExtracted() {
        return isExtracted;
    }

    public void setIsExtracted(boolean isExtracted) {
        this.isExtracted = isExtracted;
        for(Match submatch:groups){
            submatch.setIsExtracted(true);
        }

    }

    public boolean isValue(){
        return value!=null;
    }

    public String getValue() {
        return value;
    }

    public Match setValue(String value) {
        this.value = value;
        return this;
    }

    public boolean isRegex() {
        return regex;
    }

    public void setRegex(boolean regex) {
        this.regex = regex;
    }

    public boolean isGenerated() {
        return generated;
    }

    public void setGenerated(boolean generated) {
        this.generated = generated;
    }

    public static List<Match> regroup(List<Match> matchList){
        List<Match> resultList = new ArrayList<>();
        Collections.sort(matchList);

        int j =0;
        for(int i=0; i < matchList.size();){
            Match current = matchList.get(i);

            for(j=i+1;j<matchList.size(); j++) {
                Match submatch = matchList.get(j);
                if (current.contains(submatch)) {
                    current.addSubMatch(submatch);
                }
                else {
                    break;
                }
            }

            resultList.add(current);
            i=j;
        }

        return resultList;
    }

    public static List<Match> merge(List<Match> matchList){
        List<Match> resultList = new ArrayList<>();

        int j;
        boolean intersect;
        for(int i=0;i<matchList.size(); i++){
            Match match = matchList.get(i);
            intersect = false;
            for(j=i+1; j<matchList.size(); j++){
                Match next = matchList.get(j);
                intersect = match.intersect(next);
                if(intersect){
                    Match newMatch = new Match(match.label, Math.min(match.getStart(), next.getStart()), Math.max(match.getEnd(), next.getEnd()));
                    resultList.add(newMatch);
                }
                else break;
            }

            if(!intersect){
                resultList.add(match);
            }
            i = j;
        }

        return resultList;
    }

    public String matchString(String text){
        return text.substring(start, end);
    }

    @Override
    public String toString() {
        return "Match{" +
                "label='" + label + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
