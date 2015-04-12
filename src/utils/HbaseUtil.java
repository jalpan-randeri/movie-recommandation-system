package utils;

import conts.TableConts;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class HbaseUtil {
    /**
     * get the list of all the movies this user has rated
     * @param user_id String as user_id which is key in hbase table
     * @return String representing the list of all the movie,rating $ separated.
     * @throws IOException
     */
    public static String getMoviesList(HTableInterface table, String user_id) throws IOException {
        Get query = new Get(Bytes.toBytes(user_id));
        query.setMaxVersions(1);
        Result row = table.get(query);

        List<KeyValue> movies = row.getColumn(Bytes.toBytes(TableConts.TABLE_USR_MOV_COL_FAMILY),
                Bytes.toBytes(TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV));

        return Bytes.toString(movies.get(0).getValue());
    }
}
