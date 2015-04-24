package populate.hbase;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 *
 * this will reade the dataset.csv file
 * and generate the HBase table -> TABLE_NAME_TRAIN
 *
 * id,  user_id, movie_id, watch_date, rating
 *
 * Created by jalpanranderi on 4/24/15.
 */
public class HPopulateTrainigTable {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();


        if(otherArgs.length != 1){
            System.out.println("Usage : HPopulateTrainigTable : <input/ dataset.csv>");
            System.exit(1);
        }

        generateTable(conf);

        Job job = new Job(conf, "Hpopulate movies table");
        job.setJarByClass(HPopulateTrainigTable.class);
        job.setMapperClass(HTraingMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_TRAIN);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    private static void generateTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_TRAIN);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_TRAIN));
        HBaseAdmin admin = new HBaseAdmin(co);

        if(admin.tableExists(TableConts.TABLE_NAME_TRAIN)){
            admin.disableTable(TableConts.TABLE_NAME_TRAIN);
            admin.deleteTable(TableConts.TABLE_NAME_TRAIN);
        }

        admin.createTable(hd);
        admin.close();
    }


    public static class HTraingMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        private CSVParser mParser = new CSVParser();
        private HTable mTable;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_TRAIN);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = mParser.parseLine(value.toString());


            String row_key = tokens[MovieConts.INDEX_KEY];

            Put row = new Put(row_key.getBytes());
            row.add(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAING_USER_ID.getBytes(),
                    tokens[MovieConts.INDEX_CUST_ID].getBytes());

            row.add(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_MOVIE_ID.getBytes(),
                    tokens[MovieConts.INDEX_MOVIE_ID].getBytes());

            row.add(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_WATCH_DATE.getBytes(),
                    tokens[MovieConts.INDEX_MOVIE_RATING_YEAR].getBytes());

            row.add(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_RATING.getBytes(),
                    tokens[MovieConts.INDEX_RATING].getBytes());
            mTable.put(row);



        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
        }
    }

}
