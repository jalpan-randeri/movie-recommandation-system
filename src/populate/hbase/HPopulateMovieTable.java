package populate.hbase;

import com.opencsv.CSVParser;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
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
 * It will create the HBase table
 *
 * TABLE_NAME_MOVIES =
 *
 * movie_id, movie_name, movie_realease_year
 *
 *
 * Created by jalpanranderi on 4/24/15.
 */
public class HPopulateMovieTable {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

        if(otherArgs.length != 1){
            System.out.println("Usage : HPopulateMovieTable : <input/ movies_titles.txt>");
            System.exit(1);
        }

        generateTable(conf);


        Job job = new Job(conf, "Hpopulate movies table");
        job.setJarByClass(HPopulateMovieTable.class);
        job.setMapperClass(HMoviesMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_MOVIES);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    private static void generateTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_MOVIES);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_MOVIES));
        HBaseAdmin admin = new HBaseAdmin(co);

        if(admin.tableExists(TableConts.TABLE_NAME_MOVIES)){
            admin.disableTable(TableConts.TABLE_NAME_MOVIES);
            admin.deleteTable(TableConts.TABLE_NAME_MOVIES);
        }

        admin.createTable(hd);
        admin.close();
    }


    public static class HMoviesMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        private CSVParser mParser = new CSVParser();
        private HTable mTable;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_MOVIES);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = mParser.parseLine(value.toString());

            Put row = new Put(tokens[MovieConts.INDEX_R_MOVIE_ID].getBytes());

            row.add(TableConts.FAMILY_TBL_MOVIES.getBytes(),
                    TableConts.COL_TBL_MOVIES_NAME.getBytes(),
                    tokens[MovieConts.INDEX_R_MOVIE_NAME].getBytes());

            row.add(TableConts.FAMILY_TBL_MOVIES.getBytes(),
                    TableConts.COL_TBL_MOVIES_YEAR.getBytes(),
                    tokens[MovieConts.INDEX_R_MOVIE_YEAR].getBytes());

            mTable.put(row);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
        }
    }
}
