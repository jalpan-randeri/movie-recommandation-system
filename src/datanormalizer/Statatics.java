package datanormalizer;

import com.opencsv.CSVParser;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class Statatics {
    public static void main(String[] args) throws IOException {

        HashMap<String, List<Integer>> avg_usr_rating = new HashMap<>();
        CSVParser parser = new CSVParser();

        HashMap<String, List<Integer>> avg_rating = new HashMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream("dataset.csv")));
        String line;
        String name = null;

        while ((line = reader.readLine()) != null) {
            if (line.contains(":")) {
                name = line.replace(":", "");
            } else {

                String[] tokens = parser.parseLine(line);
                List<Integer> lst = null;

                if (avg_usr_rating.containsKey(name)) {
                    lst = avg_usr_rating.get(tokens[1]);
                    lst.add(getYear(tokens[3]));
                    avg_usr_rating.put(tokens[1], lst);


                    lst = avg_rating.get(tokens[1]);
                    lst.add(getRating(tokens[2]));
                    avg_rating.put(tokens[1], lst);
                } else {
                    lst = new ArrayList<>();
                    lst.add(getYear(tokens[3]));
                    avg_usr_rating.put(tokens[1], lst);

                    lst = new ArrayList<>();
                    lst.add(getRating(tokens[2]));
                    avg_rating.put(tokens[1], lst);
                }
            }
        }

        reader.close();



        System.out.println("User Rating Average");
        printAverage(avg_rating, "user_rating_average.txt");
        System.out.println("Movie Rating_year_average");
        printAverage(avg_usr_rating, "movie_average.txt");

    }

    private static void printAverage(HashMap<String, List<Integer>> map, String filename) throws IOException {
//        FileWriter writer = new FileWriter(filename);

        HashMap<Double, Integer> count_map = new HashMap<>();

        for(String key : map.keySet()){
            double avg = getAverage(map.get(key));

            if(count_map.containsKey(avg)){
                count_map.put(avg, count_map.get(avg) + 1);
            }else{
                count_map.put(avg, 0);
            }
//            writer.write(str);
//            System.out.printf(str);
        }

//        writer.close();


        for(Double key: count_map.keySet()){
            System.out.printf("%.0f, %d\n",key, count_map.get(key));
        }
    }

    private static double getAverage(List<Integer> list){
        double sum = 0;
        long count = 0;
        for(int i : list){
            sum = sum + i;
            count++;
        }

        return sum/count;
    }



    private static Integer getRating(String token) {
        return Integer.parseInt(token);
    }


    private static int getYear(String s) {
        return Integer.parseInt(s.split("-")[0]);
    }
}
