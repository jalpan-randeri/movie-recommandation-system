package hbase_populate;

import conts.TableConts;
import hbase_populate.comparators.KeyComparator;
import hbase_populate.comparators.UserGrouper;
import hbase_populate.mappers.HMapper;
import hbase_populate.model.MovKey;
import hbase_populate.reducers.HReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class PopulateUserMovieTable {




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

        job.setMapperClass(HMapper.class);
        job.setReducerClass(HReducer.class);
        job.setNumReduceTasks(10);

        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(UserGrouper.class);

        job.setOutputKeyClass(MovKey.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TableOutputFormat.class);


        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_USR_MOV);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * generate HBase table required for population in HBase
     * @param conf Configuration
     * @throws IOException
     */
    private static void createHbaseTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        createTable(admin, TableConts.TABLE_NAME_USR_MOV, TableConts.FAMILY_USR_MOV);
        createTable(admin, TableConts.TABLE_NAME_USR_RATING, TableConts.FAMILY_USR_RATING);

        admin.close();
    }

    /**
     * create table for the given name and column family
     * @param admin HBase Admin to create a table
     * @param table_name String table name
     * @param family String column family name
     * @throws IOException
     */
    private static void createTable(HBaseAdmin admin, String table_name, String family) throws IOException {
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_USR_MOV);
        hd.addFamily(new HColumnDescriptor(family));
        if(admin.tableExists(table_name)){
            admin.disableTable(table_name);
            admin.deleteTable(table_name);
        }
        admin.createTable(hd);
    }
















}
