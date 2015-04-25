package rjoin.hdfs;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.HashMap;

/**
 * Created by jalpanranderi on 4/19/15.
 */
public class Replicated {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage Replicated <Distributed Cache> <Input> <Output>");
            System.exit(1);
        }

        //  0 - Distributed Cache
        //  1 - Input file
        //  2 - Output directory
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);

        Job job = new Job(conf, "Replicated join to populate database");
        job.setJarByClass(Replicated.class);
        job.setNumReduceTasks(10);
        job.setMapperClass(HMoviesMapper.class);
        job.setReducerClass(HMoviesReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(YearRatingNameValue.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class HMoviesMapper extends Mapper<LongWritable, Text, LongWritable, YearRatingNameValue> {

//        private HashMap<String, String> mCachedNames = new HashMap<>();
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
//                mCachedNames.put(tokens[MovieConts.INDEX_R_MOVIE_ID], tokens[MovieConts.INDEX_R_MOVIE_NAME]);
                mCachedYear.put(tokens[MovieConts.INDEX_R_MOVIE_ID],
                        Integer.parseInt(tokens[MovieConts.INDEX_R_MOVIE_YEAR]));
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = mParser.parseLine(value.toString());

            long emit_key = Long.parseLong(tokens[MovieConts.INDEX_MOVIE_CUST_ID]);

            int release_year = mCachedYear.get(tokens[MovieConts.INDEX_MOVIE_ID]);
            int watch_year = getYear(tokens[MovieConts.INDEX_MOVIE_RATING_YEAR]);


            double rating = Integer.parseInt(tokens[MovieConts.INDEX_MOVIE_RATING]) - 2.5;
//            String name = mCachedNames.get(tokens[MovieConts.INDEX_MOVIE_ID]);

            YearRatingNameValue emmit_value = new YearRatingNameValue(watch_year, release_year, rating, tokens[MovieConts.INDEX_MOVIE_ID]);

            context.write(new LongWritable(emit_key), emmit_value);
        }


        /**
         * get watch_year will return the watch_year form the string
         *
         * @param year String date representation as yyyy-mm-dd
         * @return int watch_year
         */
        private int getYear(String year) {
            return Integer.parseInt(year.substring(0, 4));
        }

    }


    public static class HMoviesReducer extends Reducer<LongWritable, YearRatingNameValue, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<YearRatingNameValue> values, Context context) throws IOException, InterruptedException {
            // 1. get the data from the list
            double total_watch_year = 0;
            double total_release_year = 0;
            double total_rating = 0;
            long count = 0;

            StringBuilder movies = new StringBuilder();
            for (YearRatingNameValue v : values) {
                total_watch_year = total_watch_year + v.watch_year;
                total_rating = total_rating + v.rating.get();
                total_release_year = total_release_year + v.release_year;
                count++;
                movies.append(v.name);
                movies.append(DatasetConts.SEPARATOR);
            }
            movies.deleteCharAt(movies.length() - 1);

            double avg_rating = Math.round(total_rating / count);
            double avg_watch_year = Math.round(total_watch_year / count);
            double avg_release_year = Math.round(total_release_year / count);

            context.write(key, generateValue(avg_rating, avg_watch_year, avg_release_year, movies.toString()));
        }

        /**
         * generate value will create a string for avg_rating, avg_year, movie_list
         *
         * @param avg_rating  Long average rating
         * @param avg_watch    Long average watch_year
         * @param avg_realease    Long average watch_year
         * @param movies_list String
         * @return Text
         */
        private Text generateValue(double avg_rating, double avg_watch, double avg_realease, String movies_list) {
            return new Text(String.format("%.2f,%.2f,%.2f,%s", avg_rating, avg_watch, avg_realease, movies_list));
        }
    }


    public static class YearRatingNameValue implements WritableComparable<YearRatingNameValue> {

        public int watch_year;
        public DoubleWritable rating;
        public int release_year;
        public String name;

        public YearRatingNameValue() {
            watch_year = 0;
            rating = new DoubleWritable(0);
            release_year = 0;
            name = null;
        }

        public YearRatingNameValue(int watch_year,  int release_year, double rating, String mName) {
            this.watch_year = watch_year;
            this.rating = new DoubleWritable(rating);
            this.name = mName;
            this.release_year = release_year;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof YearRatingNameValue)) return false;

            YearRatingNameValue ratYKey = (YearRatingNameValue) o;

            if (watch_year != ratYKey.watch_year) return false;
            if (rating != ratYKey.rating) return false;
            return !(name != null ? !name.equals(ratYKey.name) : ratYKey.name != null);

        }

        @Override
        public int hashCode() {
            int result = watch_year;
            result = (int) (31 * result + rating.get());
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeVInt(dataOutput, watch_year);
            rating.write(dataOutput);
            WritableUtils.writeVInt(dataOutput, release_year);
            WritableUtils.writeString(dataOutput, name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            watch_year = WritableUtils.readVInt(dataInput);
            rating.readFields(dataInput);
            release_year = WritableUtils.readVInt(dataInput);
            name = WritableUtils.readString(dataInput);
        }

        @Override
        public int compareTo(YearRatingNameValue o) {
            if (!name.equals(o.name)) {
                return name.compareTo(o.name);
            } else {
                return Integer.compare(watch_year, o.watch_year) == 0 ?
                        Double.compare(rating.get(), o.rating.get()) : 0;
            }
        }
    }
}
