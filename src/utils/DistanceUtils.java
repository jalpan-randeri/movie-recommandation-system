package utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by jalpanranderi on 4/11/15.
 */
public class DistanceUtils {

    public static double cosineSimilarity(List<String> list1, List<String> list2){
        Collections.sort(list1, new SortingComparator());
        Collections.sort(list2, new SortingComparator());

        // get the common integers
        //

        return 0;
    }

    public static class SortingComparator implements Comparator<String>{

        @Override
        public int compare(String o1, String o2) {
            return 0;
        }
    }
}
