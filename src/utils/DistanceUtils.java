package utils;

import conts.DatasetConts;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jalpanranderi on 4/11/15.
 */
public class DistanceUtils {

    /**
     * cosineSimilarity will give the similarity between two lists
     *
     * @param sorted_list1  String (Movie_id,Rating$)
     * @param sorted_list2  String (Movie_id,Rating$)
     * @return double [0-1]
     */
    public static double cosineSimilarity(List<String> sorted_list1, List<String> sorted_list2){

        // 1. get the common movie_id and multiply their corresponding ratings
        double common = getCommons(sorted_list1, sorted_list2);

        // 2. divide the 1 by absolute value of sorted_list1 + sorted_list2
        return common / (absolute(sorted_list1) + absolute(sorted_list2));
    }


    /**
     * get the absolute value of a vector using this formula
     * A = (1,2,3)
     * |A| = Sqrt(1*1 + 2*2 + 3*3)
     *
     * @param sorted_list1
     * @return
     */
    private static double absolute(List<String> sorted_list1) {
        double sum = 0;
        for(String item : sorted_list1){
            double rat = getRating(item);
            sum = sum + (rat * rat);
        }
        return Math.sqrt(sum);
    }


    /**
     * getRating extract the rating from the given value tokens.
     * @param token String representing (Movie_id,Rating$)
     * @return int rating square;
     */
    private static int getRating(String token) {
        return Integer.parseInt(token.split(DatasetConts.SEPRATOR_VALUE)[1]
                .replace(DatasetConts.SEPRATOR_ITEM, ""));
    }

    /**
     * Extract the Movie Id form the given value tokens.
     * @param token String in form of (Movie_id,Rating$)
     * @return Long representing movie_id
     */
    private static long getMovie(String token){
        return Long.parseLong(token.split(DatasetConts.SEPRATOR_VALUE)[0]);
    }


    /**
     * get multiplication of common movies in two lists
     *
     * @param sorted_list1 sorted List[String] representing movie,rating
     * @param sorted_list2 sorted List[String] representing movie,rating
     * @return Double the common movie rating multiplication.
     */
    private static double getCommons(List<String> sorted_list1, List<String> sorted_list2) {
        double common = 0;

        Iterator<String> ptr1 = sorted_list1.iterator();
        Iterator<String> ptr2 = sorted_list2.iterator();

        String item1 = null;
        String item2 = null;

        while(ptr1.hasNext() && ptr2.hasNext()){
            if(item1 == null){
                item1 = ptr1.next();
            }

            if(item2 == null){
                item2 = ptr2.next();
            }

            if(isMovieSame(item1, item2)){
                if(common == 0){
                    common = getRating(item1) * getRating(item2);
                }else{
                    common = common * getRating(item1) * getRating(item2);
                }
            }

            if(isItem1GreaterThanIte2(item1, item2)){
                item2 = ptr2.next();
            }else{
                item1 = ptr1.next();
            }
        }

        return common;
    }

    /**
     * isItemGreaterThanItem2 returns true iff item1.movie_id > item2.movie_id
     * @param item1 String (Movie_id,Rating$)
     * @param item2 String (Movie_id,Rating$)
     * @return Boolean
     */
    private static boolean isItem1GreaterThanIte2(String item1, String item2) {
        return getMovie(item1) > getMovie(item2);
    }

    /**
     * true iff both movie are same
     * @param item1 String (Movie_id,Rating$)
     * @param item2 String (Movie_id,Rating$)
     * @return Boolean
     */
    private static boolean isMovieSame(String item1, String item2){
        return getMovie(item1) == getMovie(item2);
    }


}
