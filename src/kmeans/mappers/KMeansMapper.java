package kmeans.mappers;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.KMeansConts;
import conts.MovieConts;
import conts.TableConts;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DistanceUtils;

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
        mConnection =  HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME.getBytes());
        mCentroidsTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());

        List<String> centroids = CentroidUtils.getCentroids(mCentroidsTable, KMeansConts.K);
        for(String c : centroids){
            centroid_movies.put(c, getMoviesList(c));
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
        if(tokens.length == 4){

            String list_ip = getMoviesList(tokens[MovieConts.INDEX_CUST_ID]);

            String nearest = null;
            double closest = -2;
            int i = 0;
            int emit_id = 0;
            for(String centroid : centroid_movies.keySet()) {
                String list_user = centroid_movies.get(centroid);
                i++;
                double similarity = DistanceUtils.cosineSimilarity(list_ip.split(DatasetConts.SEPRATOR_ITEM),
                        list_user.split(DatasetConts.SEPRATOR_ITEM));

                if(similarity > closest){
                    nearest = centroid;
                    closest = similarity;
                    emit_id = i;
                }
            }

            context.write(new Text(nearest+"$"+emit_id), new Text(tokens[MovieConts.INDEX_CUST_ID]));

        }

    }

    /**
     * get the list of all the movies this user has rated
     * @param user_id String as user_id which is key in hbase table
     * @return String representing the list of all the movie,rating $ separated.
     * @throws IOException
     */
    private String getMoviesList(String user_id) throws IOException {
        Get query = new Get(Bytes.toBytes(user_id));
        query.setMaxVersions(1);
        Result row = mTable.get(query);

        List<KeyValue> movies = row.getColumn(Bytes.toBytes(TableConts.TABLE_USR_MOV_COL_FAMILY),
                Bytes.toBytes(TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV));

        return Bytes.toString(movies.get(0).getValue());
    }
}
