package rjoin.hbase;


import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.HashMap;


/**
 *
 * Read dataset.csv file and use the replicated join to convert the dataset
 * into
 *
 * user_id, avg_rating, avg_watch_year, avg_release_year, list_movies
 *
 * this will create the table in hbase. TABLE_NETFLIX_DATASET
 *
 * Created by jalpanranderi on 4/19/15.
 */
public class HPopulateMovies {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();




        generateTable(conf);

        Job job = new Job(conf, "Replicated join to populate database");
        job.setJarByClass(HPopulateMovies.class);
        job.setNumReduceTasks(10);
        job.setMapperClass(HMoviesMapper.class);
        job.setReducerClass(HMoviesReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(YearRatingNameValue.class);

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);


        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_DATASET);


        Scan scan = new Scan();
        scan.addFamily(TableConts.FAMILY_TBL_TRAIN.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_TRAIN.getBytes(), TableConts.COL_TBL_TRAIN_MOVIE_ID.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_TRAIN.getBytes(), TableConts.COL_TBL_TRAING_USER_ID.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_TRAIN.getBytes(), TableConts.COL_TBL_TRAIN_RATING.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_TRAIN.getBytes(), TableConts.COL_TBL_TRAIN_WATCH_DATE.getBytes());
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_TRAIN,
                scan,
                HMoviesMapper.class,
                LongWritable.class,
                YearRatingNameValue.class,
                job);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Generate HBase table for the data set storage
     * @param conf Configuration
     */
    private static void generateTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_DATASET);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_DATASET));
        HBaseAdmin admin = new HBaseAdmin(co);

        if(admin.tableExists(TableConts.TABLE_NAME_DATASET)){
            admin.disableTable(TableConts.TABLE_NAME_DATASET);
            admin.deleteTable(TableConts.TABLE_NAME_DATASET);
        }

        admin.createTable(hd);
        admin.close();
    }


    public static class HMoviesMapper extends TableMapper<LongWritable, YearRatingNameValue> {

        private HashMap<String, String> mCachedNames = new HashMap<>();
        private HashMap<String, Integer> mCachedYear = new HashMap<>();
        private CSVParser mParser = new CSVParser();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            readTable();
        }



        private void readTable() throws IOException {
            HTable mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_MOVIES);

            // Instantiating the Scan class
            Scan scan = new Scan();



            // Scanning the required columns
            scan.addColumn(TableConts.FAMILY_TBL_MOVIES.getBytes(), TableConts.COL_TBL_MOVIES_NAME.getBytes());
            scan.addColumn(TableConts.FAMILY_TBL_MOVIES.getBytes(), TableConts.COL_TBL_MOVIES_YEAR.getBytes());

            // Getting the scan result
            ResultScanner scanner = mTable.getScanner(scan);

            // Reading values from scan result

            for (Result result : scanner) {

                byte[] cb = result.getRow();
                byte[] c_name = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_MOVIES),
                        Bytes.toBytes(TableConts.COL_TBL_MOVIES_NAME));

                byte[] c_year = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_MOVIES),
                        Bytes.toBytes(TableConts.COL_TBL_MOVIES_YEAR));

                String id = Bytes.toString(cb);
                int year = Integer.parseInt(Bytes.toString(c_year));
                String name = Bytes.toString(c_name);

                mCachedNames.put(id, name);
                mCachedYear.put(id, year);

            }
            //closing the scanner
            scanner.close();
            mTable.close();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {



            // 1. read the current row
            KeyValue keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_MOVIE_ID.getBytes());
            String movie_id = Bytes.toString(keyValue.getValue());


            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAING_USER_ID.getBytes());
            int user_id = Integer.parseInt(Bytes.toString(keyValue.getValue()));


            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_RATING.getBytes());
            int rating = Integer.parseInt(Bytes.toString(keyValue.getValue()));


            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_TRAIN.getBytes(),
                    TableConts.COL_TBL_TRAIN_WATCH_DATE.getBytes());
            int rating_year = getYear(Bytes.toString(keyValue.getValue()));



            int release_year = mCachedYear.get(movie_id);
            String name = mCachedNames.get(movie_id);
            YearRatingNameValue emmit_value = new YearRatingNameValue(rating_year,release_year , rating, name);

            context.write(new LongWritable(user_id), emmit_value);
        }




        /**
         * get year will return the year form the string
         *
         * @param year String date representation as yyyy-mm-dd
         * @return int year
         */
        private int getYear(String year) {
            return Integer.parseInt(year.substring(0, 4));
        }

    }


    public static class HMoviesReducer extends Reducer<LongWritable, YearRatingNameValue, LongWritable, Text> {

        private HTable mTable;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_DATASET);
            mTable.setAutoFlush(true);
            mTable.setWriteBufferSize(TableConts.MB_100);
        }



        @Override
        protected void reduce(LongWritable key, Iterable<YearRatingNameValue> values, Context context) throws IOException, InterruptedException {

            // 1. get the data from the list
            double total_watch_year = 0;
            double total_release_year = 0;
            double total_rating = 0;
            long count = 0;

            StringBuilder movies = new StringBuilder();
            for (YearRatingNameValue v : values) {
                total_watch_year = total_watch_year + v.year;
                total_rating = total_rating + v.watch_rating;
                total_release_year = total_release_year + v.release_year;
                count++;
                movies.append(v.name);
                movies.append(DatasetConts.SEPARATOR);
            }
            movies.deleteCharAt(movies.length() - 1);

            double avg_rating = Math.round(total_rating / count);
            double avg_watch_year = Math.round(total_watch_year / count);
            double avg_release_year = Math.round(total_release_year / count);



            // 2. add the item into the HBase table
            Put row = new Put(String.valueOf(key.get()).getBytes());
            row.add(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_AVG_RATING.getBytes(),
                    String.valueOf(avg_rating).getBytes());

            row.add(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR.getBytes(),
                    String.valueOf(avg_watch_year).getBytes());

            row.add(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_MOVIE_LIST.getBytes(),
                    movies.toString().getBytes());

            row.add(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_AVG_RELEASE_YEAR.getBytes(),
                    String.valueOf(avg_release_year).getBytes());

            mTable.put(row);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTable.close();
        }


    }


    public static class YearRatingNameValue implements WritableComparable<YearRatingNameValue> {

        public int year;
        public int watch_rating;
        public int release_year;
        public String name;

        public YearRatingNameValue() {
            year = 0;
            watch_rating = 0;
            release_year = 0;
            name = null;
        }

        public YearRatingNameValue(int mYear,  int mReleaseYear, int mRating, String mName) {
            this.year = mYear;
            this.watch_rating = mRating;
            this.name = mName;
            this.release_year = mReleaseYear;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof YearRatingNameValue)) return false;

            YearRatingNameValue ratYKey = (YearRatingNameValue) o;

            if (year != ratYKey.year) return false;
            if (watch_rating != ratYKey.watch_rating) return false;
            return !(name != null ? !name.equals(ratYKey.name) : ratYKey.name != null);

        }

        @Override
        public int hashCode() {
            int result = year;
            result = 31 * result + watch_rating;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeVInt(dataOutput, year);
            WritableUtils.writeVInt(dataOutput, watch_rating);
            WritableUtils.writeVInt(dataOutput, release_year);
            WritableUtils.writeString(dataOutput, name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            year = WritableUtils.readVInt(dataInput);
            watch_rating = WritableUtils.readVInt(dataInput);
            release_year = WritableUtils.readVInt(dataInput);
            name = WritableUtils.readString(dataInput);
        }

        @Override
        public int compareTo(YearRatingNameValue o) {
            if (!name.equals(o.name)) {
                return name.compareTo(o.name);
            } else {
                return Integer.compare(year, o.year) == 0 ?
                        Integer.compare(watch_rating, o.watch_rating) : 0;
            }
        }
    }
}
