package kmeans.reducers;

import conts.TableConts;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class KMeansReducer extends
        Reducer<Text, Text, Text, Text> {

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

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        // 1 retrieve the cluster index
        String id = key.toString().split("\\$")[1];

        // 2. get all the members
        int count = 0;
        ArrayList<Long> list = new ArrayList<>();
        for(Text t : values){
            count++;
            list.add(Long.parseLong(t.toString()));
        }

        // 3. find the new centroid as median of cluster
        String centroid = String.valueOf(getMedian(list, count/2));

        // 4. save centroid
        saveCentroid(id, centroid);

        // 5. save members
        addToCluster(mClusters, list.toString(), id);
    }

    /**
     * save the given centroid into the centroid table
     * @param id String centroid id
     * @param centroid String centroid
     * @throws IOException
     */
    private void saveCentroid(String id, String centroid) throws IOException {
        Put row = new Put(id.getBytes());
        row.add(TableConts.FAMILY_CENTROID.getBytes(),
                TableConts.KEY_NEW_CENTROID_CENTROID.getBytes(), centroid.getBytes());
        mTable.put(row);
    }

    /**
     * write members to cluster tables
     * @param table HBase table interface
     * @param memebrs String members
     * @param id String cluster id
     * @throws IOException
     */
    private void addToCluster(HTableInterface table, String memebrs, String id) throws IOException {
        Put row = new Put(id.getBytes());
        row.add(TableConts.FAMILY_CLUSTERS.getBytes(),
                TableConts.TABLE_CLUSTERS_COLUMN_MEMBERS.getBytes(),
                memebrs.getBytes());
        table.put(row);
    }

    /**
     * return the median element of the list
     * @param list List[Long] user ids
     * @param median_index int median index
     * @return Long user id
     */
    private long getMedian(ArrayList<Long> list, int median_index) {
        return list.get(median_index);
    }


}


