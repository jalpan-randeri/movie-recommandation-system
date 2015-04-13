package datanormalizer;

import java.io.*;

/**
 * Created by jalpanranderi on 4/8/15.
 */
public class FileJoiner {
    public static void main(String[] args) throws IOException {

        File folder = new File("/Users/jalpanranderi/Documents/netflix-dataset/training_set/");
        FileWriter writer = new FileWriter("dataset.csv");
        for (File f : folder.listFiles()) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(f)));

            String line;
            String name = null;
            while ((line = reader.readLine()) != null) {
                if (line.contains(":")) {
                    name = line.replace(":","");
                } else {
                    writer.write(String.format("%s,%s\n", name, line));
                }
            }

            reader.close();
        }
        writer.close();
        System.out.println("Finished");
    }
}
