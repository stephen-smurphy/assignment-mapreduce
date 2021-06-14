
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Scanner;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class MapReduceFiles {

    public static void main(String[] args) {

        if (args.length < 5) {
          System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt linesPerThread wordsPerThread");
        }

        double linesPerThread = Double.parseDouble(args[3]);
        double wordsPerThread = Double.parseDouble(args[4]);

        Map<String, String> input = new HashMap<String, String>();
        try {
            input.put(args[0], readFile(args[0]));
            input.put(args[1], readFile(args[1]));
            input.put(args[2], readFile(args[2]));
        }
        catch (IOException ex) {
            System.err.println("Error reading files...\n" + ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }
        
        long startTime1 = System.nanoTime();        
        // APPROACH #1: Brute force
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                String[] words = contents.trim().split("\\s+");

                for(String word : words) {

                    Map<String, Integer> files = output.get(word);
                    if (files == null) {
                        files = new HashMap<String, Integer>();
                        output.put(word, files);
                    }

                    Integer occurrences = files.remove(file);
                    if (occurrences == null) {
                        files.put(file, 1);
                    } else {
                        files.put(file, occurrences.intValue() + 1);
                    }
                }
            }

            //show me:
            //System.out.println(output);
        }
        long elapsedTime1 = System.nanoTime() - startTime1;
        System.out.println("Approach 1 Execution Time: " + elapsedTime1/1000000 + " ms");

        long startTime2 = System.nanoTime();
        // APPROACH #2: MapReduce
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                map(file, contents, mappedItems);
            }

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduce(word, list, output);
            }

            //System.out.println(output);
        }
        long elapsedTime2 = System.nanoTime() - startTime2;
        System.out.println("Approach 2 Execution Time: " + elapsedTime2/1000000 + " ms");
        
        long startTime3 = System.nanoTime();
        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                double lines = 1;
                Matcher m = Pattern.compile("\n").matcher(contents);
                while (m.find()) {
                    lines ++;
                }

                String[] groupedLines = contents.split("\n", (int)Math.ceil(lines/linesPerThread));
                for(String s : groupedLines) {
                    //System.out.println("Starting thread (File: " + file + ")");
                    Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            map(file, s, mapCallback);
                        }
                    });
                    mapCluster.add(t);
                    t.start();
                }
            }

            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(List<String> words, List<Map<String, Integer>> reducedListGroup) {
                    for(int i=0;i<words.size();i++) {
                        String k = words.get(i);
                        Map<String, Integer> v = reducedListGroup.get(i);
                        output.put(k, v);
                    }
                }
            };

            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();

            //Creates entries list and iterates over each entry to add to list
            List<Map.Entry<String, List<String>>> entries = new ArrayList<Map.Entry<String, List<String>>>();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                entries.add(entry);
            }


            //Calculates number of threads needed based on how many words per thread user requests and the amount of entries
            double threads = (int)Math.ceil(entries.size() / wordsPerThread);

            //Divides up entries to different threads and starts the threads
            for(int i =0;i<threads;i++) {
                List<Map.Entry<String, List<String>>> threadEntries = new ArrayList<Map.Entry<String, List<String>>>();
                for(int j=(int)(i*wordsPerThread);j<(i+1)*wordsPerThread;j++) {
                    threadEntries.add(entries.get(j));
                    //for last thread, entries left may be less than wordsPerThread, stops when max has been reached
                    if(j == entries.size() - 1) { 
                        break;
                    }
                }

                //Starts thread and sends group of entries to be reduced
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(threadEntries, reduceCallback);
                    }
                });
                reduceCluster.add(t);
                t.start();
            }

            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            //System.out.println(output);
            
            
            /*//Write output to file
            try {
                FileOutputStream outputStream = new FileOutputStream("output.txt");
                byte[] strToBytes = output.toString().getBytes();
                outputStream.write(strToBytes);
                outputStream.close();
            } catch(IOException e) {
                e.printStackTrace();
            }*/
        }
        long elapsedTime3 = System.nanoTime() - startTime3;
        System.out.println("Approach 3 Execution Time: " + elapsedTime3/1000000 + " ms");
        //Write timing to file
        FileWriter fw;
        try {
            fw = new FileWriter("times.txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(linesPerThread + "," + wordsPerThread + "," + Long.toString(elapsedTime1/1000000) + "," + Long.toString(elapsedTime2/1000000) + "," + Long.toString(elapsedTime3/1000000));
            bw.newLine();
            bw.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
  
    //Method 2 Map Method
    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }
    
    //Method 2 Reduce Method
    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {
        public void mapDone(E key, List<V> values);
    }
  
    //Approach 3 Map Method
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            //Remove Punctuation from words
            String filteredWord = word.replaceAll("\\p{Punct}", "");
            //Remove numbers from words
            filteredWord = filteredWord.replaceAll("\\d+", "");
            results.add(new MappedItem(filteredWord, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {
        public void reduceDone(List<String> e, List<Map<String, Integer>> results);
    }
    
    //Approach 3 Reduce Method
    public static void reduce(List<Map.Entry<String, List<String>>> threadEntries, ReduceCallback<String, String, Integer> callback) {
	List<Map<String, Integer>> reducedListGroup = new ArrayList<Map<String, Integer>>();
	List<String> words = new ArrayList<String>();
	for(Map.Entry<String, List<String>> entry : threadEntries) {
            String word = entry.getKey();
            List<String> list = entry.getValue();
            Map<String, Integer> reducedList = new HashMap<String, Integer>();
            for(String file: list) {
                Integer occurrences = reducedList.get(file);
                if (occurrences == null) {
                    reducedList.put(file, 1);
                } else {
                    reducedList.put(file, occurrences.intValue() + 1);
                }
            }
            reducedListGroup.add(reducedList);
            words.add(word);
	}
	callback.reduceDone(words, reducedListGroup);
    }

    private static class MappedItem {
        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }
}
