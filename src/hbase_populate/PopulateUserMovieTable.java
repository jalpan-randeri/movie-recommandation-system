package hbase_populate;

import com.opencsv.CSVParser;
import conts.DatasetConts;
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jalpanranderi on 4/9/15.
 */
public class PopulateUserMovieTable {


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

        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(UserGroupper.class);

        job.setOutputKeyClass(MovKey.class);
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


    public static class PreMapper extends Mapper<Object, Text, MovKey, Text>{

        private CSVParser parser = new CSVParser();


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = parser.parseLine(value.toString());

            if(tokens.length == 4) {
                MovKey mKey = new MovKey(tokens[MovieConts.INDEX_CUST_ID], tokens[MovieConts.INDEX_MOVIE_ID]);
                context.write(mKey, new Text(tokens[MovieConts.INDEX_RATING]));
            }
        }

    }


    public static class MovKey implements WritableComparable<MovKey>{

        public String user;
        public String movie_id;



        public MovKey() {
            user = "";
            movie_id = "";
        }

        public MovKey(String user, String movie_id) {
            this.user = user;
            this.movie_id = movie_id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MovKey)) return false;

            MovKey movKey = (MovKey) o;

            if (!user.equals(movKey.user)) return false;
            return movie_id.equals(movKey.movie_id);

        }

        @Override
        public int hashCode() {
            int result = user.hashCode();
            result = 31 * result + movie_id.hashCode();
            return result;
        }

        @Override
        public int compareTo(MovKey o) {
            long this_usr = Long.parseLong(user);
            long that_user = Long.parseLong(o.user);
            int ans = Long.compare(this_usr, that_user);

            if(ans == 0) {
                long this_mov = Long.parseLong(movie_id);
                long that_mov = Long.parseLong(o.movie_id);

                return Long.compare(this_mov, that_mov);
            }

            return ans;
        }



        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput, user);
            WritableUtils.writeString(dataOutput, movie_id);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            user = WritableUtils.readString(dataInput);
            movie_id = WritableUtils.readString(dataInput);
        }
    }



    public static class UserGroupper extends WritableComparator{

        protected UserGroupper() {
            super(MovKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            MovKey key1 = (MovKey) a;
            MovKey key2 = (MovKey) b;

            long u1 = Long.parseLong(key1.user);
            long u2 = Long.parseLong(key2.user);

            return Long.compare(u1, u2);
        }

    }


    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(MovKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            MovKey key1 = (MovKey) a;
            MovKey key2 = (MovKey) b;

            return key1.compareTo(key2);
        }
    }

    public static class PreReducer extends Reducer<MovKey, Text, ImmutableBytesWritable, Writable>{

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
        protected void reduce(MovKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text v : values){
                builder.append(key.movie_id);
                builder.append(DatasetConts.SEPRATOR_VALUE);
                builder.append(v.toString());
                builder.append(DatasetConts.SEPRATOR_ITEM);
            }


            Put row = new Put(Bytes.toBytes(key.toString()));
            row.add(TableConts.TABLE_USR_MOV_COL_FAMILY.getBytes(),
                    TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV.getBytes(), builder.toString().getBytes());

            mTable.put(row);


            builder.setLength(0);

        }
    }

}
