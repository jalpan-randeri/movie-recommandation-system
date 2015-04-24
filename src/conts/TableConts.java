package conts;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class TableConts {

    public static final String TABLE_NAME_USR_RATING = "TABLE_USR_RATING";
    public static final String FAMILY_USR_RATING = "ratings";
    public static final String KEY_USR_RATING = "USR_ID";
    public static final String TABLE_COL_RATING ="RATING";

    // User Movie watch_rating table
    public static final String TABLE_NAME_USR_MOV = "TABLE_USR_MOV_MAPPING";
    public static final String FAMILY_USR_MOV = "movies";
    public static final String KEY_USR_MOV_USR = "USR_ID";
    public static final String TABLE_USR_MOV_COLUMN_LIST_MOV = "LIST_MOV";

    // KMeans centroid table
    public static final String TABLE_NAME_CENTROID = "TABLE_USR_CENTROID";
    public static final String FAMILY_CENTROID = "centroids_family";
//    public static final String KEY_CENTROID_COL_ID = "CENTROID_ID";
    public static final String COL_TBL_CENTROID_COL_X = "X";
    public static final String COL_TBL_CENTROID_COL_Y = "Y";

    // KMeans new centroids table
    public static final String TABLE_NAME_NEW_CENTROID = "TABLE_NEW_USR_CENTROID";
    public static final String FAMILY_NEW_CENTROID = "centroids_family";
//    public static final String KEY_NEW_CENTROID_CENTROID = "CENTROID_ID";
    public static final String COL_TBL_NEW_CENTROID_COL_X = "X";
    public static final String COL_TBL_NEW_CENTROID_COL_Y = "Y";

    // KMeans user cluster membership table
    public static final String TABLE_NAME_CLUSTERS = "TABLE_USR_CLUSTER";
    public static final String FAMILY_CLUSTERS = "clusters";
    public static final String COL_TBL_CLUSTERS__CLUSTER_ID = "CLUSTER_ID";
    public static final String COL_TBL_CLUSTERS_MEMBERS = "CLUSTER_MEMBERS";


    public static final String TABLE_NAME_DATASET = "TABLE_NETFLIX_DATASET";
    public static final String FAMILY_TBL_DATASET = "netflix_dataset";
    public static final String KEY_TBL_DATASET = "KEY_USR_ID";
    public static final String COL_TBL_DATASET_AVG_RATING = "AVG_RATING";
    public static final String COL_TBL_DATASET_AVG_WATCHED_YEAR = "AVG WATCH YEAR";
    public static final String COL_TBL_DATASET_AVG_RELEASE_YEAR = "AVG RELEASE YEAR";
    public static final String COL_TBL_DATASET_MOVIE_LIST = "MOVIES";
    public static final String COL_TBL_DATASET_MEMBERSHIP = "MEMBERSHIP";

    public static final String TABLE_NAME_KNN = "TABLE_NAME_KNN";
    public static final String FAMILY_TBL_KNN = "netflix_dataset";
    public static final String KEY_TBL_KNN = "KEY_USR_ID";
    public static final String COL_TBL_KNN_AVG_WATCHED_YEAR = "AVG WATCH YEAR";
    public static final String COL_TBL_KNN_AVG_RELEASE_YEAR = "AVG RELEASE YEAR";
    public static final String COL_TBL_KNN_MOVIE_LIST = "MOVIES";
    public static final String COL_TBL_KNN_MEMBERSHIP = "MEMBERSHIP";



    public static final String TABLE_NAME_TRAIN = "TABLE_NAME_TRAIN";
    public static final String FAMILY_TBL_TRAIN = "training";
    public static final String KEY_TBL_TRAIN = "KEY_USR";
    public static final String COL_TBL_TRAIN_MOVIE_ID = "MOVIE ID";
    public static final String COL_TBL_TRAIN_WATCH_DATE = "WATCH DATE";
    public static final String COL_TBL_TRAIN_RATING = "RATING";


    public static final int MB_100 = 102400;
}
