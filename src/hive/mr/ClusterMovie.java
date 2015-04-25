package hive.mr;

import conts.DatasetConts;
import conts.TableConts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * This will create the table TABLE_NAME_CLUSTER_MOVIES
 * <p/>
 * cluster_id, movie_id
 * <p/>
 * <p/>
 * Created by jalpanranderi on 4/24/15.
 */
public class ClusterMovie {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        createTable(conf);

        Job job = new Job(conf, "Cluster Movie");
        job.setJarByClass(ClusterMovie.class);
        job.setMapperClass(CMoiveMapper.class);

        job.setNumReduceTasks(0);


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TableOutputFormat.class);


        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_CLUSTER_MOVIES);

        Scan scan = new Scan();
        scan.addFamily(TableConts.FAMILY_TBL_DATASET.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_MOVIE_LIST.getBytes());
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_DATASET,
                scan,
                CMoiveMapper.class,
                NullWritable.class,
                NullWritable.class,
                job);


        job.waitForCompletion(true);
    }


    private static void createTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        // main centroids locations
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_CLUSTER_MOVIES);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_CLUSTER_MOVIES));
        if (admin.tableExists(TableConts.TABLE_NAME_CLUSTER_MOVIES)) {
            admin.disableTable(TableConts.TABLE_NAME_CLUSTER_MOVIES);
            admin.deleteTable(TableConts.TABLE_NAME_CLUSTER_MOVIES);
        }
        admin.createTable(hd);
        admin.close();
    }


    public static class CMoiveMapper extends TableMapper<NullWritable, NullWritable> {

        private HTable mTable;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_CLUSTER_MOVIES);
            mTable.setAutoFlush(false);

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // 1. read the cluster id
            KeyValue keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());
            String cluster_id = Bytes.toString(keyValue.getValue());

            // 2. read the movie ids
            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_MOVIE_LIST.getBytes());
            String movies = Bytes.toString(keyValue.getValue());

            // 3. write it to hbase table
            for (String m : movies.split(DatasetConts.SEPARATOR)) {
                Put row = new Put(String.format("%s,%s", cluster_id, m).getBytes());
                row.add(TableConts.FAMILY_TBL_CLUSTER_MOVIES.getBytes(),
                        TableConts.COL_TBL_CLUSTER_MOVIES_CLUSTER.getBytes(),
                        cluster_id.getBytes());
                row.add(TableConts.FAMILY_TBL_CLUSTER_MOVIES.getBytes(),
                        TableConts.COL_TBL_CLUSTER_MOVIES_MOVIE.getBytes(),
                        m.getBytes());
                mTable.put(row);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
        }
    }
}
