package membership;

import conts.DatasetConts;
import conts.TableConts;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jalpanranderi on 4/23/15.
 */
public class CSVExporter {
    public static void main(String[] args) throws IOException {


        FileWriter writer = new FileWriter("netflix.csv");


        HTable mDataset = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_DATASET);

        // Instantiating the Scan class
        Scan scan = new Scan();
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_AVG_RATING.getBytes());
        // Scanning the required columns
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_AVG_RELEASE_YEAR.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());

        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(),
                TableConts.COL_TBL_DATASET_MOVIE_LIST.getBytes());


        // Getting the scan result
        ResultScanner scanner = mDataset.getScanner(scan);


        // Reading values from scan result
        int count = 0;
        for (Result result : scanner) {
            count++;

            byte[] cb = result.getRow();
            String user_id = Bytes.toString(cb);

            byte[] avg_cb = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                    Bytes.toBytes(TableConts.COL_TBL_DATASET_AVG_RATING));
            String avg_rating = Bytes.toString(avg_cb);

            byte[] avg_watch_cb = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                    Bytes.toBytes(TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR));
            String avg_watch = Bytes.toString(avg_watch_cb);

            byte[] avg_relese = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                    Bytes.toBytes(TableConts.COL_TBL_DATASET_AVG_RELEASE_YEAR));
            String avg_relasae_year = Bytes.toString(avg_relese);

            byte[] membership = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                    Bytes.toBytes(TableConts.COL_TBL_DATASET_MEMBERSHIP));
            String flag = Bytes.toString(membership);

            byte[] c_movies = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_DATASET),
                    Bytes.toBytes(TableConts.COL_TBL_DATASET_MOVIE_LIST));
            String movies = Bytes.toString(c_movies);

            String s = String.format("%s,%s,%s,%s,%s,\"%s\"\n", user_id, flag, avg_rating, avg_watch, avg_relasae_year, movies);
            System.out.println(s);
            writer.write(s);
            if(count == 100)
                break;
        }
        //closing the scanner
        scanner.close();

        writer.close();
    }
}
