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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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
 * Created by jalpanranderi on 4/19/15.
 */
public class HPopulateMovies {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.out.println("Usage HPopulateMovies <Distributed Cache> <Input>");
            System.exit(1);
        }

        //  0 - Distributed Cache
        //  1 - Input file
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);

        generateTable(conf);

        Job job = new Job(conf, "Replicated join to populate database");
        job.setJarByClass(HPopulateMovies.class);
        job.setNumReduceTasks(10);
        job.setMapperClass(HMoviesMapper.class);
        job.setReducerClass(HMoviesReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(YearRatingNameValue.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_DATASET);

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


    public static class HMoviesMapper extends Mapper<LongWritable, Text, LongWritable, YearRatingNameValue> {

        private HashMap<String, String> mCachedNames = new HashMap<>();
        private HashMap<String, Integer> mCachedYear = new HashMap<>();
        private CSVParser mParser = new CSVParser();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (cacheFile != null && cacheFile.length > 0) {
                readFile(cacheFile[0].toString());
            }
        }

        /**
         * read file reads the file which is distributed and added into the HashMap
         *
         * @param path input file path
         */
        private void readFile(String path) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = mParser.parseLine(line);
                mCachedNames.put(tokens[MovieConts.INDEX_R_MOVIE_ID], tokens[MovieConts.INDEX_R_MOVIE_NAME]);
                mCachedYear.put(tokens[MovieConts.INDEX_R_MOVIE_ID],
                        Integer.parseInt(tokens[MovieConts.INDEX_R_MOVIE_YEAR]));
            }
            reader.close();

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = mParser.parseLine(value.toString());

            long emit_key = Long.parseLong(tokens[MovieConts.INDEX_CUST_ID]);

            int release_year = mCachedYear.get(tokens[MovieConts.INDEX_R_MOVIE_ID]);
            int rating_year = getYear(tokens[MovieConts.INDEX_MOVIE_RATING_YEAR]);


            int rating = Integer.parseInt(tokens[MovieConts.INDEX_RATING]);
            String name = mCachedNames.get(tokens[MovieConts.INDEX_MOVIE_ID]);
            YearRatingNameValue emmit_value = new YearRatingNameValue(rating_year,release_year , rating, name);

            context.write(new LongWritable(emit_key), emmit_value);
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
