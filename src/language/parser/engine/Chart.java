package language.parser.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wolf on 26.06.2015.
 */
public class Chart implements Serializable {
    private int size;
    private Cell[][] table;

    public Chart(List<Match> matchList) {
        this.size = matchList.size();
        this.table = new Cell[size][size];

        for (int i = 0; i < size; i++) {
            Match match = matchList.get(i);
            List<Match> submatches = match.getGroups();
            table[i][0] = new Cell(i, 0, matchList.get(i));
            for (Match submatch : submatches) {
                table[i][0].addMatch(submatch);
            }
        }

        for (int i = 0; i < size; i++) {
            for (int j = 1; j < size; j++) {
                table[i][j] = new Cell(i, j);
            }
        }
    }

    public Cell getCell(int i, int j) {
        return this.table[i][j];
    }

    public List<Match> chartResult() {
        List<Match> matchList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            for (int j = size - 1; j >= 0; j--) {

                Cell cell = table[i][j];

                if (!cell.isEmpty() /*&& !cell.isExtracted()*/) {
                    List<Match> cellMatches = cell.getMatches();
                    boolean ifAny = false;
                    for (int k = 0; k < cellMatches.size(); k++) {
                        Match cellMatch = cellMatches.get(k);
                        if (!cellMatch.containsExtracted() && !cellMatch.isGenerated() && !cellMatch.isRegex()) {
                            matchList.add(cellMatch);
                            cellMatch.setIsExtracted(true);
                            ifAny = true;
                        }
                    }

                    ///if(ifAny) cell.setExtracted(true);
                }
            }
        }
        return Match.regroup(matchList);
    }

    private boolean alreadyRangeIn(List<Match> matchList, Match newMatch){

        for(Match insideMatch:matchList){
            if(insideMatch.intersect(newMatch)) return false;
        }

        return true;
    }


    private String printSpace(int i, int preLength) {
        String text = "";
        for (int k = 0; k < i - preLength; k++) text += " ";

        return text;
    }

    public void printChart() {
        for (int i = 0; i < size; i++) {
            String row = "";
            for (int j = 0; j < size; j++) {
                Cell cell = table[i][j];
                row += printSpace(10, row.length()) + cell.toString();
            }

            System.out.println(row);
        }
    }

    public void printChart(Integer spacing) {
        for (int i = 0; i < size; i++) {
            String row = "";
            Integer prevLength = 0;
            for (int j = 0; j < size; j++) {
                Cell cell = table[i][j];
                String cellString = cell.toString();
                row += printSpace(spacing, prevLength) + cellString;
                prevLength = cellString.length();
            }

            System.out.println(row);
        }
    }

}
