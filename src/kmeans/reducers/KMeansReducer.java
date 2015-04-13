package kmeans.reducers;

import conts.TableConts;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mConnection =  HConnectionManager.createConnection(context.getConfiguration());
        mTable = mConnection.getTable(TableConts.TABLE_NAME_NEW_CENTROID.getBytes());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mTable.close();
        mConnection.close();
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {



        int count = 0;
        long sum = 0;
        ArrayList<Long> list = new ArrayList<>();
        for(Text t : values){
            if(!t.toString().contains("NA")) {
                count++;
                list.add(Long.parseLong(t.toString()));
            }
        }


        String id = key.toString().split("\\$")[1];


        if(count > 0) {
            String centroid = String.valueOf(getMedian(list, count/2));
            Put row = new Put(id.getBytes());
            row.add(TableConts.TABLE_CENTROID_FAMAILY.getBytes(),
                    TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID.getBytes(), centroid.getBytes());
            mTable.put(row);
        }else {
            Put row = new Put(id.getBytes());
            String centroid = key.toString().split("\\$")[0];
            row.add(TableConts.TABLE_CENTROID_FAMAILY.getBytes(),
                    TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID.getBytes(), centroid.getBytes());
            mTable.put(row);
        }

//        context.write(new Text(String.valueOf(id)), new Text(centroid));
    }

    private long getMedian(ArrayList<Long> list, int median_index) {
        return list.get(median_index);
    }


}


