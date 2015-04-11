package datanormalizer;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by jalpanranderi on 4/10/15.
 */
public class KMeansIterative {
    public static void main(String[] args) {

    }

    public static final String SEP_ITEM = "$";
    public static final String SEP_VAL = ",";

    private static double cosinSimilarity(List<String> user, List<String> test){
        sort(user);
        sort(test);
//        double dot_product = getDotProductOfCommon(user, test);
//        return dot_product / (absolute(user) * absolute(test));
        return 0;
    }

    private static double getDotProductOfCommon(List<String> user, List<String> test) {


        return 0;
    }

    private static void sort(List<String> list){
        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
               int id1 = Integer.parseInt(o1.split(",")[0]);
                int id2 = Integer.parseInt(o2.split(",")[0]);
               return Integer.compare(id1, id2);
            }
        });
    }


}
