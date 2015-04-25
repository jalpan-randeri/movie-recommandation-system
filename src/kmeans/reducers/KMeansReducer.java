package kmeans.reducers;

import conts.DatasetConts;
import conts.TableConts;
import kmeans.model.EmitValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class KMeansReducer extends
        Reducer<IntWritable, EmitValue, Text, Text> {

    HConnection mConnection;
    HTableInterface mTable;
    HTableInterface mClusters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // setup connection with HBase and cluster table and new centroid table
        mConnection =  HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME_NEW_CENTROID.getBytes());
        mClusters = mConnection.getTable(TableConts.TABLE_NAME_CLUSTERS.getBytes());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // clear connection
        mTable.close();
        mClusters.close();
        mConnection.close();
    }

    @Override
    public void reduce(IntWritable key, Iterable<EmitValue> values,
                       Context context) throws IOException, InterruptedException {
        // 1 retrieve the cluster index
        int cluster_id = key.get();

        // 2. get all the members
        // TODO: something is not working here
        HashMap<String,EmitValue> list = new HashMap<>();
        double total_year = 0;
        double total_rating = 0;
        for(EmitValue t : values){
//            System.out.println(t.user_id);
            list.put(t.user_id, t);
            total_year = total_year + t.year.get();
            total_rating = total_rating + t.rating.get();
        }

        // 3. find the new centroid as average of cluster
        int new_rating = (int) (total_rating / list.size());
        int new_year = (int) (total_year / list.size());


        // 4. save centroid
        saveCentroid(String.valueOf(cluster_id), new_rating, new_year);

        // 5. save members
        // TODO: move this into cleanup phase
        addToCluster(mClusters, list, String.valueOf(cluster_id));
    }

    /**
     * save the given centroid into the centroid table
     * @param id String centroid id
     * @throws IOException
     */
    private void saveCentroid(String id, int x, int y) throws IOException {
        Put row = new Put(id.getBytes());
        row.add(TableConts.FAMILY_CENTROID.getBytes(),
                TableConts.COL_TBL_CENTROID_COL_X.getBytes(), String.valueOf(x).getBytes());
        row.add(TableConts.FAMILY_CENTROID.getBytes(),
                TableConts.COL_TBL_CENTROID_COL_Y.getBytes(), String.valueOf(y).getBytes());
        mTable.put(row);
    }

    /**
     * write members to cluster tables
     * @param table HBase table interface
     * @param list List[EmitValue] members
     * @param id String cluster id
     * @throws IOException
     */
    private void addToCluster(HTableInterface table, HashMap<String, EmitValue> list, String id) throws IOException {

        StringBuilder builder = new StringBuilder();
        for(String e : list.keySet()){
            builder.append(e);
            builder.append(DatasetConts.SEPARATOR);
        }

        Put row = new Put(id.getBytes());
        row.add(TableConts.FAMILY_CLUSTERS.getBytes(),
                TableConts.COL_TBL_CLUSTERS_MEMBERS.getBytes(),
                builder.toString().getBytes());
        table.put(row);
    }




}


