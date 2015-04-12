package hbase_populate;

import com.opencsv.CSVParser;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class PopulateUserMovieTable {

    public static final String SEPRATOR_VALUE = ",";
    public static final String SEPRATOR_ITEM = "$";
    private static final int MB_100 = 102400;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 1) {
            System.err.println("Usage: populate.PreprocessorNetflix <input>");
            System.exit(1);
        }

        createHbaseTable(conf);


        Job job = new Job(conf, "Populate Data into HBase Table");
        job.setJarByClass(PopulateUserMovieTable.class);

        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setNumReduceTasks(10);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TableOutputFormat.class);


        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void createHbaseTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME);
        hd.addFamily(new HColumnDescriptor(TableConts.TABLE_USR_MOV_COL_FAMILY));
        HBaseAdmin admin = new HBaseAdmin(co);

        if(admin.tableExists(TableConts.TABLE_NAME)){
            admin.disableTable(TableConts.TABLE_NAME);
            admin.deleteTable(TableConts.TABLE_NAME);
        }

        admin.createTable(hd);
        admin.close();
    }


    public static class PreMapper extends Mapper<Object, Text, Text, Text>{

        private CSVParser parser = new CSVParser();


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = parser.parseLine(value.toString());

            if(tokens.length == 4) {
                String out = String.format("%s,%s", tokens[MovieConts.INDEX_MOVIE_ID], tokens[MovieConts.INDEX_RATING]);
                context.write(new Text(tokens[MovieConts.INDEX_CUST_ID]),
                        new Text(out));
            }
        }

    }



    public static class PreReducer extends Reducer<Text, Text, ImmutableBytesWritable, Writable>{

        private StringBuilder builder = new StringBuilder();
        private HTable mTable;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME);
            mTable.setAutoFlush(true);
            mTable.setWriteBufferSize(MB_100);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mTable.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text v : values){
                builder.append(v.toString());
                builder.append(SEPRATOR_ITEM);
            }


            Put row = new Put(Bytes.toBytes(key.toString()));
            row.add(TableConts.TABLE_USR_MOV_COL_FAMILY.getBytes(),
                    TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV.getBytes(), builder.toString().getBytes());

            mTable.put(row);


            builder.setLength(0);

        }
    }

}
