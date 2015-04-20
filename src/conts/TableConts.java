package conts;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class TableConts {

    public static final String TABLE_NAME_USR_RATING = "TABLE_USR_RATING";
    public static final String FAMILY_USR_RATING = "ratings";
    public static final String KEY_USR_RATING = "USR_ID";
    public static final String TABLE_COL_RATING ="RATING";

    // User Movie rating table
    public static final String TABLE_NAME_USR_MOV = "TABLE_USR_MOV_MAPPING";
    public static final String FAMILY_USR_MOV = "movies";
    public static final String KEY_USR_MOV_USR = "USR_ID";
    public static final String TABLE_USR_MOV_COLUMN_LIST_MOV = "LIST_MOV";

    // KMeans centroid table
    public static final String TABLE_NAME_CENTROID = "TABLE_USR_CENTROID";
    public static final String FAMILY_CENTROID = "centroids_family";
    public static final String KEY_CENTROID_COL_ID = "CENTROID_ID";
    public static final String TABLE_CENTROID_COL_CENTROID = "CENTROID_VALUE";

    // KMeans new centroids table
    public static final String TABLE_NAME_NEW_CENTROID = "TABLE_NEW_USR_CENTROID";
    public static final String FAMILY_NEW_CENTROID = "centroids_family";
    public static final String KEY_NEW_CENTROID_CENTROID = "CENTROID_ID";
    public static final String TABLE_NEW_CENTROID_COL_CENTROID = "CENTROID_VALUE";

    // KMeans user cluster membership table
    public static final String TABLE_NAME_CLUSTERS = "TABLE_USR_CLUSTER";
    public static final String FAMILY_CLUSTERS = "clusters";
    public static final String TABLE_CLUSTERS_COL_CLUSTER_ID = "CLUSTER_ID";
    public static final String TABLE_CLUSTERS_COLUMN_MEMBERS = "CLUSTER_MEMBERS";
}
