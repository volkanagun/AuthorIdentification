package language.morphology;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wolf on 09.01.2016.
 */
public class CommandSentenceTrMorph {

    private String poses = "[N|V|Det|Adj|Adv|Prn|Punc|Cnj|Num|Sym]";

    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Stream Wrapper Thread">
    private class StreamWrapper extends Thread {
        InputStream is = null;
        String type = null;
        StringBuffer message;
        boolean isReady = false;

        public String getMessage() {
            return message.toString();
        }

        StreamWrapper(InputStream is, String type) {
            this.is = is;
            this.type = type;
        }

        public void run() {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                message = new StringBuffer();
                String line = null;
                while ((line = br.readLine()) != null) {
                    message.append(line).append("\n");
                }
                isReady = true;
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public boolean isReady() {
            return isReady;
        }
    }

    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    private String[] tokens;
    private ProcessBuilder processBuilder;
    private File directory;
    private BufferedWriter inputWriter;
    private StreamWrapper outputReaderWrapper;
    private BufferedReader outputReader, errorReader;
    private Process process;
    private Locale locale = new Locale("tr");

    private List<String> resultList;

    public CommandSentenceTrMorph(String directory, String[] tokens, List<String> resultList) {
        this.tokens = tokens;
        this.directory = new File(directory).getAbsoluteFile();
        this.resultList = resultList;
    }



    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //<editor-fold defaultstate="collapsed" desc="Obtain Morphological Results">

    private void extract(String message) {
        String[] lines = message.split("\n");
        String input, result = null, in = "foma[1]:", pa = in + " up ";

        int start = 10;


        for (int i = start; i < lines.length - 1; i++) {
            String line = lines[i];
            int index = line.indexOf(in);
            if (index != -1 && result != null) {
                resultList.add(result.trim());
                input = line.substring(index + pa.length());
                result = input + "\n";
            } else if (index != -1) {
                input = line.substring(index + pa.length());
                result = input + "\n";
            } else {
                result += line.trim() + "\n";
            }
        }


        resultList.add(result.trim());


    }



    private List<MorphResult> obtainPostResult() {
        List<MorphResult> morphResults = new ArrayList<>();
        Pattern pattern = Pattern.compile("<((\\p{L}|\\d)+)((\\:(\\p{L}|\\d+)+)+)?>");
        Pattern posregex = Pattern.compile("<("+poses+")((\\:\\p{L}+)+)?>");

        for (String tokenResult : resultList) {
            String[] lines = tokenResult.split("\\n");
            String token = lines[0];
            MorphResult morphResult = new MorphResult(token);
            for (int i = 1; i < lines.length; i++) {
                String result = lines[i];
                if (!result.endsWith("?")) {
                    MorphUnit morphUnit = new MorphUnit();
                    Matcher matcher = pattern.matcher(result);
                    String postag = MorphUnit.NONE;
                    String secpos = MorphUnit.NONE;
                    if (matcher.find()) {
                        int index = matcher.start();
                        postag = matcher.group(1);
                        secpos = matcher.group(3);
                        secpos = secpos!=null?secpos:MorphUnit.NONE;
                        morphUnit.setPrimaryPos(postag);
                        morphUnit.setSecondaryPos(secpos);
                        morphUnit.setStem(result.substring(0, index));
                        morphUnit.addTag(postag,postag);
                    }

                    while (matcher.find()) {
                        String match = matcher.group();
                        String tag = match.substring(1, match.length() - 1);
                        Matcher posMatcher = posregex.matcher(match);
                        if(posMatcher.find()){
                            morphUnit.setFinalPos(posMatcher.group(1));
                            //morphUnit.setSecondaryPos(posMatcher.group(2));
                        }
                        else {
                            morphUnit.addTag(tag, tag);
                        }
                    }

                    morphResult.addMorphUnit(morphUnit);
                } else if (token.matches("[A-ZĞÜŞİÇÖ][a-zàèğüışçö]{2,}")) {
                    MorphUnit morphUnit = new MorphUnit("N:prop", token);
                    morphResult.addMorphUnit(morphUnit);
                } else if (token.matches("[A-ZĞÜŞİÇÖ][A-ZĞÜŞİÇÖa-zàèğüışçö]{2,}")) {
                    MorphUnit morphUnit = new MorphUnit("N:prop", token);
                    morphResult.addMorphUnit(morphUnit);
                } else if (token.matches("[A-ZĞÜŞİÇÖ]+")) {

                    MorphUnit morphUnit = new MorphUnit("N:prop:abbr", token);
                    morphResult.addMorphUnit(morphUnit);
                } else if (token.matches("\\p{Punct}+")) {
                    MorphUnit morphUnit = new MorphUnit("Punc", token);
                    morphResult.addMorphUnit(morphUnit);
                }
            }

            morphResults.add(morphResult);

        }

        return morphResults;

    }
    //</editor-fold>
    ///////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    private void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }


    public List<MorphResult> analyze() {

        try {
            open();
            for (String token : tokens) {
                write(token);
            }
            writeExit();
            //sleep(100);
            close();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        extract(outputReaderWrapper.getMessage());
        return obtainPostResult();
    }

    public void print() {
        for (String token : resultList) {
            System.out.println(token + "\n");
        }
    }



    private void writeExit() throws IOException {
        inputWriter.write("exit");
        inputWriter.write(13);
        inputWriter.flush();

    }

    private void write(String token) throws IOException {
        inputWriter.write("up " + token);
        inputWriter.write(13);
        inputWriter.flush();
    }


    private void open() throws IOException, InterruptedException {
        processBuilder = new ProcessBuilder("/bin/sh", "-c", "foma");
        processBuilder.directory(directory);

        process = processBuilder.start();
        inputWriter = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        outputReaderWrapper = new StreamWrapper(process.getInputStream(), "reader");
        outputReaderWrapper.start();

        inputWriter.write("regex @\"trmorph.fst\";");
        inputWriter.write(13);
        inputWriter.flush();


    }

    private void close() throws IOException, InterruptedException {
        if (processBuilder != null) {
            int val = process.waitFor();
            while (outputReaderWrapper.isAlive() || !outputReaderWrapper.isReady) {

            }
            //sleep(100);
            inputWriter.close();
            outputReader.close();
            outputReaderWrapper.interrupt();


        }
    }

    private String toUpperStart(String token) {
        char[] chars = token.toCharArray();

        if (token.length() > 0 && Character.isLowerCase(chars[0])) {
            String start = token.substring(0, 1).toUpperCase(locale);
            return start + token.substring(1);
        } else {
            return token;
        }
    }

    private String toLowercase(String token) {
        return token.toLowerCase(locale);
    }

    public static void singleTest(){

        CommandSentenceTrMorph morph = new CommandSentenceTrMorph("/home/wolf/Documents/java-projects/UnsupervisedNLP/morphology/trmorph/", new String[]{"Askerden","geldik","."},new ArrayList<String>());
        List<MorphResult> results = morph.analyze();
        for(MorphResult morphResult:results){
            System.out.println(morphResult);
        }
    }

    public static void parallelTest() throws InterruptedException, ExecutionException {
        final String[] tokens = new String[]{"insüline", "korgan", "melek", "binek", "arka", "sol", "bacak", "parıldatamadığın"};
        ExecutorService exec = Executors.newFixedThreadPool(18);
        List<Callable<Integer>> tasks = new ArrayList<>();
        for(int i=0;i<5000;i++) {
            Callable<Integer> calls = new Callable<Integer>() {

                @Override
                public Integer call() throws Exception {
                    List<String> results = new ArrayList<String>();
                    CommandSentenceTrMorph morphCommand = new CommandSentenceTrMorph("/home/wolf/Documents/java-projects/UnsupervisedNLP/morphology/trmorph/", tokens, results);
                    List<MorphResult> resultList = morphCommand.analyze();
                    int count = 0;

                    for(MorphResult morphResult:resultList){
                        if(morphResult.isEmpty()) count++;
                    }

                    return count;
                }
            };

            tasks.add(calls);
        }

        List<Future<Integer>> resu = exec.invokeAll(tasks);

        exec.shutdown();

        while(!exec.isShutdown()){

        }

        long cum = 0;
        for(Future<Integer> call:resu){
            cum += call.get();
        }

        System.out.println(cum - 5000);
    }

    public static void main(String[] args) {

        try {
            parallelTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        singleTest();

    }
}
