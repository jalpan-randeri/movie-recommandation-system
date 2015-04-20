package kmeans.utils;

import conts.TableConts;
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

    /**
     * List of previous centroids which are assigned befor the start of iteration
     *
     * @param centroidTable HBase table interface
     * @param k Integer total number of centroids
     * @return List[string] as centroids
     * @throws IOException
     */
    public static List<String> getCentroids(HTableInterface centroidTable, int k) throws IOException {
        List<String> list = null;
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = centroidTable.get(query);

            byte[] cb = row.getValue(Bytes.toBytes(TableConts.FAMILY_CENTROID),
                    Bytes.toBytes(TableConts.KEY_CENTROID_COL_ID));

            String centroid = Bytes.toString(cb);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(centroid);


        }

        return list;
    }

    /**
     * returns the list of new centroids
     * @param newCentroidTable HBase table interface
     * @param k Integer total number of centroids
     * @return List[String] as new centroids
     * @throws IOException
     */
    public static List<String> getNewCentroids(HTableInterface newCentroidTable, int k) throws IOException {
        List<String> list = null;
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = newCentroidTable.get(query);

            byte[] cb = row.getValue(Bytes.toBytes(TableConts.FAMILY_NEW_CENTROID),
                    Bytes.toBytes(TableConts.KEY_NEW_CENTROID_CENTROID));

            String centroid = Bytes.toString(cb);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(centroid);
        }
        return list;

    }

}
