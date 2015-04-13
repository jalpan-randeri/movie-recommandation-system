package knn;

import java.io.*;

/**
 * Created by mkhosla on 4/12/15.
 */
public class FetchUserId {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("/home/mkhosla/Downloads/abc.txt"));
        File file = new File("/home/mkhosla/Downloads/userIds");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        try{
            String line = br.readLine();

            while (line != null) {
                if (line.contains(",")) {
                String[] l = line.split(",");
                bw.write(l[0] + '\n');
                }
                line = br.readLine();
            }

        } finally {
            br.close();
            bw.close();
        }


    }

}
