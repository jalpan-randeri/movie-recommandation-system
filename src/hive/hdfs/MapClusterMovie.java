package hive.hdfs;

import conts.DatasetConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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

/**
 * Created by jalpanranderi on 4/25/15.
 */
public class MapClusterMovie {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 1){
            System.out.println("Usage : MapClusterMovie output/clusterMoive");
            System.exit(1);
        }


        Job job = new Job(conf, "HDFS Cluster Movie");
        job.setJarByClass(MapClusterMovie.class);
        job.setMapperClass(CluMovMapper.class);
        job.setReducerClass(CluMovReducer.class);
        job.setCombinerClass(CluMovReducer.class);

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
                CluMovMapper.class,
                Text.class,
                Text.class,
                job);


        job.waitForCompletion(true);


    }


    public static class CluMovMapper extends TableMapper<Text, Text> {


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
                context.write(new Text(cluster_id), new Text(m));
            }
        }


    }

    public static class CluMovReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for(Text t: values){
                builder.append(t.toString());
                builder.append(DatasetConts.SEPARATOR);
            }
            builder.deleteCharAt(builder.length() - 1);

            context.write(key, new Text(builder.toString()));
        }
    }
}
