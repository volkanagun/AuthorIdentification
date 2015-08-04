package reuters;

import operations.TextFile;

import java.io.File;
import java.io.Serializable;

/**
 * Created by wolf on 31.07.2015.
 */
public class ReutersExtractor implements Serializable{
    private String REUTERSSTART = "<REUTERS\\sTOPICS=\"YES\"";
    private String REUTERSEND = "</REUTORS>";
    private String SOURCEFOLDER = "resources/source/";


    public void parse(File file){
        TextFile txtFile = new TextFile(file);
        TextFile outFile = new TextFile("resources/reuters/source/reuters.txt");

        /*String line =
        txtFile.openBufferRead();
        while()*/
    }


    public static void main(String[] args){

    }


}
