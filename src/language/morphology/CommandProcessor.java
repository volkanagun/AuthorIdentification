/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package language.morphology;

import util.TextReplacer;

import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author wolf
 */
public class CommandProcessor extends Thread implements Serializable{



    private List<String> lines;
    private final StringBuffer resultBuffer;
    private String cmd;
    private String[] cmdArray;

    private final File folder;
    private final TextReplacer replacer;

    public CommandProcessor(String cmd, File folder, TextReplacer replacer, StringBuffer resultBuffer) {
        this.cmd = cmd;
        this.folder = folder;
        this.replacer = replacer;
        this.resultBuffer = resultBuffer;
    }

    public CommandProcessor(String[] cmdArray, File folder, TextReplacer replacer, StringBuffer resultBuffer) {
        this.cmdArray = cmdArray;
        this.folder = folder;
        this.replacer = replacer;
        this.resultBuffer = resultBuffer;
    }

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //    //<editor-fold defaultstate="collapsed" desc="Experimental needs modification in perl or pyton">
//    
//    public void executeCommand(TextReplacer replacer, String cmd) {
//        try {
//            //cmd = "parallel --gnu "+cmd;
//            Runtime run = Runtime.getRuntime();
//            Process pr = run.exec(cmd);
//            pr.waitFor();
//            BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
//            String line;
//            while ((line = buf.readLine()) != null) {
//                lines.add(replacer.regexReplaceAll(line));
//            }
//        } catch (InterruptedException | IOException ex) {
//            Logger.getLogger(CommandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    public static void mysleep(Long time){
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void executeWindowsCommand(){
        String result = null;
        try {

            ProcessBuilder processBuilder;
            if(cmd!=null) {
                processBuilder = new ProcessBuilder("cmd.exe", "/C", "start", cmd);
            }
            else{
                processBuilder = new ProcessBuilder(cmdArray);
            }

            processBuilder.directory(folder);
            Process pr = processBuilder.start();


            BufferedReader errorReader = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
            String mline;
            result = "";

            while ((mline = errorReader.readLine()) != null) {
                System.out.println(mline);
            }

            errorReader.close();
            pr.waitFor();



            BufferedReader reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line;
            result = "";
            while ((line = reader.readLine()) != null) {
                result += replacer.regexReplaceAll(line) + "\n";
            }

            reader.close();
            pr.destroy();

        } catch (InterruptedException | IOException ex) {

            Logger.getLogger(CommandProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (result==null || result.isEmpty()) {
            resultBuffer.setLength(0);
        } else {
            resultBuffer.append(result.substring(0, result.length() - 1));
        }
    }

    public void executeCommand() {

        String result = null;
        try {

            ProcessBuilder processBuilder;
            if(cmd!=null) {
                processBuilder = new ProcessBuilder("/bin/sh", "-c", cmd);
            }
            else{
                processBuilder = new ProcessBuilder(cmdArray);
            }

            processBuilder.directory(folder);
            Process pr = processBuilder.start();

            
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
            String mline;
            result = "";
            
            while ((mline = errorReader.readLine()) != null) {
                System.out.println(mline);
            }
            
            errorReader.close();
            pr.waitFor();
            
            
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line;
            result = "";
            while ((line = reader.readLine()) != null) {
                result += replacer.regexReplaceAll(line) + "\n";
            }
            
            reader.close();
            pr.destroy();
            
       } catch (InterruptedException | IOException ex) {
           
            Logger.getLogger(CommandProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (result==null || result.isEmpty()) {
            resultBuffer.setLength(0);
        } else {
            resultBuffer.append(result.substring(0, result.length() - 1));
        }
    }



    @Override
    public void run() {
       executeCommand();
    }

}
