package clumovies;

import conts.DatasetConts;
import conts.TableConts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by jalpanranderi on 4/25/15.
 */
public class ClusterUniqueMovie {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 1){
            System.out.println("Usage : ClusterUniqueMoive : output");
            System.exit(0);
        }

        createTable(conf);

        Job job = new Job(conf, "Cluster Movie");
        job.setJarByClass(ClusterUniqueMovie.class);
        job.setMapperClass(CmuMapper.class);
        job.setReducerClass(CmuReducer.class);
        job.setNumReduceTasks(10);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));

        Scan scan = new Scan();
        scan.addFamily(TableConts.FAMILY_TBL_DATASET.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_MOVIE_LIST.getBytes());
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_DATASET,
                scan,
                CmuMapper.class,
                Text.class,
                Text.class,
                job);


        job.waitForCompletion(true);

    }

    private static void createTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        // main centroids locations
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_CLUSTER_UNIQUE);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_CLU_UNIQUE));
        if (admin.tableExists(TableConts.TABLE_NAME_CLUSTER_UNIQUE)) {
            admin.disableTable(TableConts.TABLE_NAME_CLUSTER_UNIQUE);
            admin.deleteTable(TableConts.TABLE_NAME_CLUSTER_UNIQUE);
        }
        admin.createTable(hd);
        admin.close();
    }


    public static class CmuMapper extends TableMapper<Text, Text>{
        private HashMap<String, HashSet<String>> mClusterMovies = new HashMap<>();

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
                if(mClusterMovies.containsKey(cluster_id)){
                    HashSet<String> set = mClusterMovies.get(cluster_id);
                    set.add(m);
                }else{
                    HashSet<String> set = new HashSet<>();
                    set.add(m);
                    mClusterMovies.put(cluster_id, set);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(String cluster : mClusterMovies.keySet()){
                HashSet<String> movie = mClusterMovies.get(cluster);
                StringBuilder builder = new StringBuilder();
                for(String m : movie){
                    builder.append(m);
                    builder.append(DatasetConts.SEPARATOR);
                }
                builder.deleteCharAt(builder.length() - 1);
                context.write(new Text(cluster), new Text(builder.toString()));
            }
        }
    }

    public static class CmuReducer extends Reducer<Text, Text, ImmutableBytesWritable, ImmutableBytesWritable>{

        private HTable mTable;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_CLUSTER_UNIQUE);
            mTable.setAutoFlush(false);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>();
            for(Text t: values){
                String[] token = t.toString().split(DatasetConts.SEPARATOR);
                for(String tok: token){
                    set.add(tok);
                    if(set.size() > 10){
                        break;
                    }
                }
                if(set.size() > 10){
                    break;
                }
            }

            StringBuilder builder = new StringBuilder();
            for(String s: set) {
                builder.append(s).append(DatasetConts.SEPARATOR);
            }
            builder.deleteCharAt(builder.length() - 1);


            Put row = new Put(key.toString().getBytes());
            row.add(TableConts.FAMILY_TBL_CLU_UNIQUE.getBytes(),
                    TableConts.COL_TBL_CLU_UNIQUE_MOVIES.getBytes(),
                    builder.toString().getBytes());
            mTable.put(row);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
        }



    }

}
