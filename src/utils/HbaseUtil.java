package utils;

import conts.TableConts;
import kmeans.model.RatYear;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class HbaseUtil {
    /**
     * get the list of all the movies this user has rated
     *
     * @param user_id String as user_id which is key in hbase table
     * @return String representing the list of all the movie,rating $ separated.
     * @throws IOException
     */
    public static String getMoviesList(HTableInterface table, String user_id) throws IOException {
        Get query = new Get(Bytes.toBytes(user_id));
        query.setMaxVersions(1);
        Result row = table.get(query);

        byte[] cb = row.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                Bytes.toBytes(TableConts.COL_TBL_DATASET_MOVIE_LIST));

        return Bytes.toString(cb);
    }


    /**
     * get the average rating and movie watch_year of the user
     * @param table HTable interface to access the hbase
     * @param userid String user id
     * @return RatYear object as the pair of the average rating and average watch_year;
     * @throws IOException
     */
    public static RatYear getUserAvgRatingYear(HTableInterface table, long userid) throws IOException {
        Get query = new Get(Bytes.toBytes(userid));
        query.setMaxVersions(1);
        Result row = table.get(query);

        byte[] b_rating = row.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                Bytes.toBytes(TableConts.COL_TBL_DATASET_AVG_RATING));

        byte[] b_year = row.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                Bytes.toBytes(TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR));


        return new RatYear(Bytes.toInt(b_year), Bytes.toInt(b_rating));
    }
}
