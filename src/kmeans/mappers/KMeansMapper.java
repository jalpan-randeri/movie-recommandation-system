package kmeans.mappers;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.KMeansConts;
import conts.MovieConts;
import conts.TableConts;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DistanceUtils;
import utils.HbaseUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HConnection mConnection;
    private HTableInterface mTable;
    private HTableInterface mCentroidsTable;
    private CSVParser mParser = new CSVParser();
    private HashMap<String, String> centroid_movies = new HashMap<>();

    protected void setup(Context context) throws IOException,
            InterruptedException {
        mConnection = HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME.getBytes());
        mCentroidsTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());

        List<String> centroids = CentroidUtils.getCentroids(mCentroidsTable, KMeansConts.K);
        for (String c : centroids) {
            centroid_movies.put(c, HbaseUtil.getMoviesList(mTable, c));
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mTable.close();
        mCentroidsTable.close();
        mConnection.close();

    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = mParser.parseLine(value.toString());

        if (tokens.length == 4) {
            String list_ip = HbaseUtil.getMoviesList(mTable, tokens[MovieConts.INDEX_CUST_ID]);

            String nearest = null;
            double closest = -2;
            int i = 0;
            int emit_id = 0;
            for (String centroid : centroid_movies.keySet()) {
                String list_user = centroid_movies.get(centroid);
                i++;
                double similarity = DistanceUtils.cosineSimilarity(list_ip.split(DatasetConts.SEPRATOR_ITEM),
                        list_user.split(DatasetConts.SEPRATOR_ITEM));

                if (similarity > closest) {
                    nearest = centroid;
                    closest = similarity;
                    emit_id = i;
                }
            }

            context.write(new Text(nearest + "$" + emit_id), new Text(tokens[MovieConts.INDEX_CUST_ID]));

        }

    }


}
