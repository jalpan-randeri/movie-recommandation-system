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
     * getRating extract the watch_rating from the given value tokens.
     * @param token String representing (Movie_id,Rating$)
     * @return int watch_rating square;
     */
    private static double getRating(String token) {
        return Double.parseDouble(token.split(DatasetConts.SEPARATOR)[1]
                .replace(DatasetConts.SEPARATOR, ""));
    }

    /**
     * Extract the Movie Id form the given value tokens.
     * @param token String in form of (Movie_id,Rating$)
     * @return Long representing movie_id
     */
    private static long getMovie(String token){
        return Long.parseLong(token.split(DatasetConts.SEPARATOR)[0]);
    }


    /**
     * get multiplication of common movies in two lists
     *
     * @param sorted_list1 sorted List[String] representing movie,watch_rating
     * @param sorted_list2 sorted List[String] representing movie,watch_rating
     * @return Double the common movie watch_rating multiplication.
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


    /**
     * returns manhattan distance of two vectors
     * @param x1 Integer as Average1
     * @param y1 Integer as Rating1
     * @param x2 Integer as Average2
     * @param y2 Integer as Rating2
     * @return Integer manhattan distance of the two vectors
     */
    public static int getManhattanDistance(int x1, int y1, int x2, int y2){
        return Math.abs(x1 - x2) + Math.abs(y1 - y2);
    }


    /**
     * returns euclidean distance of two vectors
     * @param x1 double as Average1
     * @param y1 double as Rating1
     * @param x2 double as Average2
     * @param y2 double as Rating2
     * @return Integer manhattan distance of the two vectors
     */
    public static double getEuclideanDistance(double x1, double y1, double x2, double y2){
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }


}
