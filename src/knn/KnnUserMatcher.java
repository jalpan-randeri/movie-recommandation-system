package knn;

import java.io.IOException;

import conts.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**

/**
 * Created by jalpanranderi on 4/11/15.
 */
public class KnnUserMatcher {

    public static class KNNMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException {
            Configuration config = HBaseConfiguration.create();
            HTable testTable = new HTable(config, TableConts.TABLE_NAME_USR_MOV);

                Scan scan = new Scan();
                scan.addColumn(Bytes.toBytes(TableConts.FAMILY_USR_MOV), Bytes.toBytes(TableConts.KEY_USR_MOV_USR));
                scan.addColumn(Bytes.toBytes(TableConts.FAMILY_USR_MOV), Bytes.toBytes(TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV));

                byte[] family = Bytes.toBytes(TableConts.FAMILY_USR_MOV);
                byte[] qual = Bytes.toBytes("a");

                scan.addColumn(family, qual);
                ResultScanner rs = testTable.getScanner(scan);
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    byte[] valueObj = r.getValue(family, qual);
                    String val = new String(valueObj);
                    System.out.println(val);
                }

            testTable.close();
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
