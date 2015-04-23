package kmeans.mappers;

import conts.KMeansConts;
import conts.TableConts;
import hbase_populate.model.Centroid;
import kmeans.model.EmitValue;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import utils.DistanceUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class KMeansMapper extends TableMapper<IntWritable, EmitValue> {

    private HConnection mConnection;
    private HTableInterface mTable;
    private HTableInterface mCentroidsTable;
    private List<Centroid> centroids;


    protected void setup(Context context) throws IOException,
            InterruptedException {
        // 1. setup connection with HBase, initial centroid table
        mConnection = HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME_DATASET.getBytes());
        mCentroidsTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());

        // 2. save the centroid movies into HashMap for fast retrieval
        centroids = CentroidUtils.getCentroids(mCentroidsTable, KMeansConts.K);

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

        String sid = Bytes.toString(key.get());
        long id = Long.parseLong(sid);

        // 1. read the current row
        KeyValue keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_AVG_RATING.getBytes());
        double avg_rating = Double.parseDouble(Bytes.toString(keyValue.getValue()));


        keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR.getBytes());
        double avg_year = Double.parseDouble(Bytes.toString(keyValue.getValue()));

        // 2. get the closest match from the given centroid to the current user
        double closest = Integer.MAX_VALUE;
        int centroid_id = 0;
        for (int i = 0; i < centroids.size(); i++) {
            Centroid c = centroids.get(i);

            double dist = DistanceUtils.getEuclideanDistance(avg_rating, avg_year,
                    c.rating_x, c.year_y);
            if (dist < closest) {
                closest = dist;
                centroid_id = i;
            }
        }
//        System.out.println("Closest "+centroid_id);

        EmitValue e_value = new EmitValue(id, avg_rating, avg_year);

        // 3. emmit the match with corresponding id
        context.write(new IntWritable(centroid_id), e_value);

    }

}
