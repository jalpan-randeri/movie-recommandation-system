package kmeans.utils;

import conts.TableConts;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class CentroidUtils {

    public static List<String> getCentroids(HTableInterface centroidTable,int k) throws IOException {
        List<String> list = null;
        for (int i = 0; i < k; i++) {
            Get query = new Get(Bytes.toBytes(i));

            query.setMaxVersions(1);
            Result row = centroidTable.get(query);

            List<KeyValue> centorids = row.getColumn(Bytes.toBytes(TableConts.TABLE_CENTROID_FAMAILY),
                    Bytes.toBytes(TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID));

            String centroid = Bytes.toString(centorids.get(0).getValue());

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
            Get query = new Get(Bytes.toBytes(i));

            query.setMaxVersions(1);
            Result row = newCentroidTable.get(query);

            List<KeyValue> centorids = row.getColumn(Bytes.toBytes(TableConts.TABLE_NEW_CENTROID_FAMAILY),
                    Bytes.toBytes(TableConts.TABLE_NEW_CENTROID_COLUMN_ID_CENTROID));

            String centroid = Bytes.toString(centorids.get(0).getValue());

            if (list == null) {
                list = new ArrayList<>();
            }

            list.add(centroid);
        }

        return list;

    }
}
