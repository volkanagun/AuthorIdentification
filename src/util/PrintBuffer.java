package util;

import java.io.Serializable;

/**
 * Created by wolf on 04.08.2015.
 */
public class PrintBuffer implements Serializable{
    private StringBuffer buffer;

    public PrintBuffer() {
        buffer = new StringBuffer();
    }

    public void addLine(String line){
        buffer.append(line+"\n");
    }

    public void print(){
        System.out.print(buffer.toString());
    }
}
