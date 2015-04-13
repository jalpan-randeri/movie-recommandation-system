package datanormalizer;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;

import java.io.*;
import java.util.StringTokenizer;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class Clusters {
    public static void main(String[] args) throws IOException {

        CSVParser parser = new CSVParser();

        FileWriter writer = new FileWriter("cluster3.txt");

        BufferedReader reader = new BufferedReader(new FileReader("cluster3.csv"));
        String line = null;
        while((line = reader.readLine()) != null){
            String[] tokens = parser.parseLine(line);
            for(String t : tokens){
                if(!t.trim().isEmpty()) {
                    writer.write(String.format("%s\n", t.trim()));
                }
            }
        }

        writer.close();
        reader.close();
    }
}
