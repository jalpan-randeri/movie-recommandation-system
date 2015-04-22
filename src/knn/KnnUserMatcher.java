package knn;

import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

import com.opencsv.CSVParser;
import conts.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.HbaseUtil;
import org.jruby.compiler.ir.operands.Hash;


/**

/**
 * Created by jalpanranderi on 4/11/15.
 */
public class KnnUserMatcher {

    public static class KNNMapper extends Mapper<Object, Text, Text, Text> {

        private HConnection mConnection;
        private HTableInterface mTable;
        private CSVParser mParser = new CSVParser();

        protected void setup(Context context) throws IOException,
                InterruptedException {
            mConnection = HConnectionManager.createConnection(context.getConfiguration());
            mTable = mConnection.getTable(TableConts.TABLE_NAME.getBytes());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
            mConnection.close();

        }

        public void map(Object key, Text value, Context context) throws IOException {
            String movie_list = HbaseUtil.getMoviesList(mTable, value.toString());



        }
    }

    public static class KNNReducer extends Reducer<Text, Text, NullWritable, Text> {

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: KNN <in> <out>");
            System.exit(2);
        }

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        Job job = new Job(conf, "KNN");
        job.setJarByClass(KnnUserMatcher.class);
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);

    }
}
