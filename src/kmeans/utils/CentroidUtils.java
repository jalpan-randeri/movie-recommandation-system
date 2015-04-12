package kmeans.utils;

import conts.KMeansConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class CentroidUtils {

    public static List<String> getCentroids(HTableInterface centroidTable, int k) throws IOException {
        List<String> list = null;
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = centroidTable.get(query);

            byte[] cb = row.getValue(Bytes.toBytes(TableConts.TABLE_CENTROID_FAMAILY),
                    Bytes.toBytes(TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID));

            String centroid = Bytes.toString(cb);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(centroid);


        }

        return list;
    }

    public static List<String> getNewCentroids(HTableInterface newCentroidTable, int k) throws IOException {
        List<String> list = null;
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = newCentroidTable.get(query);

            byte[] cb = row.getValue(Bytes.toBytes(TableConts.TABLE_NEW_CENTROID_FAMAILY),
                    Bytes.toBytes(TableConts.TABLE_NEW_CENTROID_COLUMN_ID_CENTROID));

            String centroid = Bytes.toString(cb);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(centroid);


        }


        return list;

    }

//    public static void main(String[] args) throws IOException {
//        Configuration conf = new Configuration();
//        HConnection con = HConnectionManager.createConnection(conf);
//        HTableInterface table = con.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());
//
//        List<String> list = getCentroids(table, KMeansConts.K);
//
//        System.out.println(list);
//    }
}
