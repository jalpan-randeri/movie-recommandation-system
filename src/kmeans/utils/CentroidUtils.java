package kmeans.utils;

import conts.TableConts;
import hbase_populate.model.Centroid;
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
    public static List<Centroid> getCentroids(HTableInterface centroidTable, int k) throws IOException {
        List<Centroid> list = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = centroidTable.get(query);

            byte[] cx = row.getValue(Bytes.toBytes(TableConts.FAMILY_CENTROID),
                    Bytes.toBytes(TableConts.COL_TBL_CENTROID_COL_X));

            byte[] cy = row.getValue(Bytes.toBytes(TableConts.FAMILY_CENTROID),
                    Bytes.toBytes(TableConts.COL_TBL_CENTROID_COL_Y));

            String x = Bytes.toString(cx);
            String y = Bytes.toString(cy);

            list.add(new Centroid(Double.parseDouble(x), Double.parseDouble(y)));
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
    public static List<Centroid> getNewCentroids(HTableInterface newCentroidTable, int k) throws IOException {
        List<Centroid> list = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            Get query = new Get(String.valueOf(i).getBytes());
            query.setMaxVersions(1);
            Result row = newCentroidTable.get(query);

            byte[] cx = row.getValue(Bytes.toBytes(TableConts.FAMILY_NEW_CENTROID),
                    Bytes.toBytes(TableConts.COL_TBL_NEW_CENTROID_COL_X));
            byte[] cy = row.getValue(Bytes.toBytes(TableConts.FAMILY_NEW_CENTROID),
                    Bytes.toBytes(TableConts.COL_TBL_NEW_CENTROID_COL_Y));

            String x = Bytes.toString(cx);
            String y = Bytes.toString(cy);

            if(x != null && y != null) {
                list.add(new Centroid(Integer.parseInt(x), Integer.parseInt(y)));
            }
        }

        return list;

    }

}
