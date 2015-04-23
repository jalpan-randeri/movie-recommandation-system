package membership;

import conts.DatasetConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by jalpanranderi on 4/23/15.
 */
public class MembershipAssigner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();




        Job job = new Job(conf, "Replicated join to populate database");
        job.setJarByClass(MembershipAssigner.class);

        job.setMapperClass(MemberMapper.class);


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_DATASET,
                scan,
                MemberMapper.class,
                NullWritable.class,
                NullWritable.class,
                job);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_DATASET);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class MemberMapper extends TableMapper<NullWritable, NullWritable> {

        private HashMap<String, String> mUsers = new HashMap<>();
        private HConnection mConnection;
        private HTableInterface mTable;
        private HTableInterface mDataset;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mConnection = HConnectionManager.createConnection(context.getConfiguration());
            mTable = mConnection.getTable(TableConts.TABLE_NAME_CLUSTERS.getBytes());
            mDataset = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_DATASET);
            readTable();
        }

        /**
         * read table will create the user- cluster mapping from the table
         */
        private void readTable() throws IOException {
            // Instantiating the Scan class
            Scan scan = new Scan();
            scan.addColumn(TableConts.FAMILY_CLUSTERS.getBytes(),
                    TableConts.COL_TBL_CLUSTERS__CLUSTER_ID.getBytes());
            scan.addColumn(TableConts.FAMILY_CLUSTERS.getBytes(),
                    TableConts.COL_TBL_CLUSTERS_MEMBERS.getBytes());


            // Scanning the required columns
            scan.addColumn(Bytes.toBytes(TableConts.FAMILY_CLUSTERS), Bytes.toBytes(TableConts.COL_TBL_CLUSTERS__CLUSTER_ID));
            scan.addColumn(Bytes.toBytes(TableConts.FAMILY_CLUSTERS), Bytes.toBytes(TableConts.COL_TBL_CLUSTERS_MEMBERS));

            // Getting the scan result
            ResultScanner scanner = mTable.getScanner(scan);

            // Reading values from scan result

            for (Result result : scanner) {

                byte[] cb = result.getRow();
                byte[] c_member = result.getValue(Bytes.toBytes(TableConts.FAMILY_CLUSTERS),
                        Bytes.toBytes(TableConts.COL_TBL_CLUSTERS_MEMBERS));

                String id = Bytes.toString(cb);
                String members = Bytes.toString(c_member);

                for(String m : members.split(DatasetConts.SEPARATOR)){
                    mUsers.put(m, id);
                }
            }
            //closing the scanner
            scanner.close();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String sid = Bytes.toString(key.get());
            System.out.println(sid);

            Put row = new Put(sid.getBytes());
            row.add(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes(),
                    mUsers.get(sid).getBytes());
            mDataset.put(row);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
            mDataset.close();
            mConnection.close();
        }
    }
}
