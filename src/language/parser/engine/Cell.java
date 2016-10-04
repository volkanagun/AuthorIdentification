package language.parser.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class Cell {
    private List<Match> matches;
    private int row, col;
    private boolean extracted = false;

    public Cell(int i, int j) {
        this.row = i;
        this.col = j;
        this.matches = new ArrayList<>();
    }

    public Cell(int i, int j, Match match){
        this.row = i;
        this.col = j;
        this.matches = new ArrayList<>();
        this.matches.add(match);
    }

    public void addMatch(Match match){
        if(!matches.contains(match)) matches.add(match);
    }

    public List<Match> getMatches() {
        Collections.sort(matches);
        return matches;
    }

    public void setMatches(List<Match> matches) {
        this.matches = matches;
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }

    public boolean isEmpty(){
        return matches.isEmpty();
    }

    public boolean isExtracted() {
        return extracted;
    }

    public void setExtracted(boolean extracted) {
        this.extracted = extracted;

        for(Match match:matches){
            match.setIsExtracted(true);
        }
    }

    @Override
    public String toString() {
        String text = "";

        for(Match match:matches){
            text+= "["+match.getLabel()+"]";
        }

        return text;
    }
}
