package kmeans.mappers;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.KMeansConts;
import conts.TableConts;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import utils.DistanceUtils;
import utils.HbaseUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class KMeansMapper extends TableMapper<Text, Text> {

    private HConnection mConnection;
    private HTableInterface mTable;
    private HTableInterface mCentroidsTable;
    private CSVParser mParser = new CSVParser();
    private HashMap<String, String> centroid_movies = new HashMap<>();

    protected void setup(Context context) throws IOException,
            InterruptedException {
        // 1. setup connection with HBase, initial centroid table
        mConnection = HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME.getBytes());
        mCentroidsTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());

        // 2. save the centroid movies into HashMap for fast retrieval
        List<String> centroids = CentroidUtils.getCentroids(mCentroidsTable, KMeansConts.K);
        for (String c : centroids) {
            centroid_movies.put(c, HbaseUtil.getMoviesList(mTable, c));
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 1. close connection
        mTable.close();
        mCentroidsTable.close();
        mConnection.close();

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 1. read the current row
        String list_ip = new String(value.getRow());

        // 2. get the closest match from the given centroid to the current user
        String nearest = null;
        double closest = -2;
        int i = 0;
        int emit_id = 0;
        for (String centroid : centroid_movies.keySet()) {
            String list_user = centroid_movies.get(centroid);
            i++;
            // TODO: ArrayIndexOutOfBound Error on distance utils
            double similarity = DistanceUtils.cosineSimilarity(list_ip.split(DatasetConts.SEPRATOR_ITEM),
                    list_user.split(DatasetConts.SEPRATOR_ITEM));

            if (similarity > closest) {
                nearest = centroid;
                closest = similarity;
                emit_id = i;
            }
        }

        // 3. emmit the match with corresponding id
        context.write(new Text(nearest + "$" + emit_id), new Text(key.toString()));
    }


}
