package utils;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jalpanranderi on 4/25/15.
 */
public class FileUtils {

    public static List<String> readFile(Path path) throws IOException {

        List<String> ans = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
        String line;
        while((line = reader.readLine()) != null){
            if(!line.isEmpty()){
                ans.add(line);
            }
        }

        reader.close();
        return ans;
    }
}
