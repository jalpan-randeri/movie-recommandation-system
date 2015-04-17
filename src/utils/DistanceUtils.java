package utils;

import conts.DatasetConts;

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
    public static double cosineSimilarity(String[] sorted_list1, String[] sorted_list2){

        // 1. get the common movie_id and multiply their corresponding ratings
        double common = getCommons(sorted_list1, sorted_list2);

        // 2. divide the 1 by absolute value of sorted_list1 * sorted_list2
        return  common / (absolute(sorted_list1) * absolute(sorted_list2));
    }


    /**
     * get the absolute value of a vector using this formula
     * A = (1,2,3)
     * |A| = Sqrt(1*1 + 2*2 + 3*3)
     *
     * @param sorted_list1 String[] tokens
     * @return
     */
    private static double absolute(String[] sorted_list1) {
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
    private static double getRating(String token) {
        return Double.parseDouble(token.split(DatasetConts.SEPARATOR_VALUE)[1]
                .replace(DatasetConts.SEPRATOR_ITEM, ""));
    }

    /**
     * Extract the Movie Id form the given value tokens.
     * @param token String in form of (Movie_id,Rating$)
     * @return Long representing movie_id
     */
    private static long getMovie(String token){
        return Long.parseLong(token.split(DatasetConts.SEPARATOR_VALUE)[0]);
    }


    /**
     * get multiplication of common movies in two lists
     *
     * @param sorted_list1 sorted List[String] representing movie,rating
     * @param sorted_list2 sorted List[String] representing movie,rating
     * @return Double the common movie rating multiplication.
     */
    private static double getCommons(String[] sorted_list1, String[] sorted_list2) {
        double common = 0;

        int ptr1 = 0;
        int ptr2 = 0;

        while(ptr1 < sorted_list1.length && ptr2 < sorted_list2.length){

            String item1 = sorted_list1[ptr1];
            String item2 = sorted_list2[ptr2];

            if(isMovieSame(item1, item2)){
                common = common + (getRating(item1) * getRating(item2));
            }

            if(isItem1GreaterThanIte2(item1, item2)){
                ptr2++;
            }else{
                ptr1++;
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


    public static void main(String[] args) {
        String[] list1 = {"1,4.75", "2,4.5", "3,5", "4,4.25", "5,4"};
        String[] list2 = {"1,4", "2,3", "3,5", "4,2", "5,1"};

        System.out.println(cosineSimilarity(list1, list2));
    }

}
