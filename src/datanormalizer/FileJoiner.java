package datanormalizer;

import java.io.*;

/**
 * Created by jalpanranderi on 4/8/15.
 */
public class FileJoiner {
    public static void main(String[] args) throws IOException {

        File folder = new File("/Users/jalpanranderi/Documents/netflix-dataset/training_set/");
        FileWriter writer = new FileWriter("dataset.csv");
        for(File f : folder.listFiles()) {
                String name = f.getName().replace(".txt","");
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(f)));
                String line = reader.readLine();
                int count = 100;
                while ((line = reader.readLine()) != null) {
                    writer.write(String.format("%s,%s\n",name, line));

                    if (count < 0) {
                        break;
                    }
                    count++;
                }

            reader.close();
        }
        writer.close();
        System.out.println("Finished");
    }
}
