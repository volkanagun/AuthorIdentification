package parser;

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

        for(int i=0;i<size; i++){
            Match match = matchList.get(i);
            List<Match> submatches = match.getGroups();
            table[i][0] = new Cell(i,0,matchList.get(i));
            for(Match submatch:submatches){
                table[i][0].addMatch(submatch);
            }
        }

        for(int i=0; i<size;i++){
            for(int j=1; j<size; j++){
                table[i][j] = new Cell(i,j);
            }
        }
    }

    public Cell getCell(int i, int j){
        return this.table[i][j];
    }

    public List<Match> chartResult(){
        List<Match> matchList = new ArrayList<>();
        for(int i=0; i<size; i++){
            for(int j=size-1; j>=0; j--){

                Cell cell = table[i][j];

                if(!cell.isEmpty())
                {
                    List<Match> cellMatches = cell.getMatches();

                    for(int k = cellMatches.size()-1; k>=0; k--){
                        Match cellMatch = cellMatches.get(k);
                        if(!cellMatch.isExtracted() && !cellMatch.isGenerated() && !cellMatch.isRegex()){
                            matchList.add(cellMatch);
                            cellMatch.setIsExtracted(true);
                        }
                    }
                    cell.setExtracted(true);
                }
            }
        }
        return Match.regroup(matchList);
    }


    private String printSpace(int i){
        String text = "";
        for(int k=0; k<i; k++) text+="\t";

        return text;
    }

    public void printChart(){
        for(int i=0; i<size; i++){
            String row = "";
            for(int j=0; j<size; j++){
                Cell cell = table[i][j];
                row+=printSpace(10)+cell.toString();
            }

            System.out.println(row);
        }
    }

}
