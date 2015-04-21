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

//            int movie_year = mCachedYear.get(tokens[MovieConts.INDEX_R_MOVIE_ID]);
            int rating_year = getYear(tokens[MovieConts.INDEX_MOVIE_RATING_YEAR]);
            int year = rating_year; //- movie_year;

            double rating = Integer.parseInt(tokens[MovieConts.INDEX_RATING]) - 2.5;
            String name = mCachedNames.get(tokens[MovieConts.INDEX_MOVIE_ID]);
            YearRatingNameValue emmit_value = new YearRatingNameValue(year, rating, name);

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

        @Override
        protected void reduce(LongWritable key, Iterable<YearRatingNameValue> values, Context context) throws IOException, InterruptedException {
            double avg_year = 0;
            long count = 0;
            double avg_rating = 0;

            StringBuilder builder = new StringBuilder();
            for (YearRatingNameValue v : values) {
                avg_year = avg_year + v.year;
                avg_rating = avg_rating + v.rating.get();
                count++;
                builder.append(v.name);
                builder.append(DatasetConts.SEPARATOR);
            }

            avg_rating = avg_rating / count;
            avg_year = avg_year / count;

            context.write(key, generateValue(avg_rating, avg_year, builder.toString()));
        }

        /**
         * generate value will create a string for avg_rating, avg_year, movie_list
         *
         * @param avg_rating  Long average rating
         * @param avg_year    Long average year
         * @param movies_list String
         * @return Text
         */
        private Text generateValue(double avg_rating, double avg_year, String movies_list) {
            return new Text(String.format("%.2f, %.2f, %s", avg_rating, avg_year, movies_list));
        }
    }


    public static class YearRatingNameValue implements WritableComparable<YearRatingNameValue> {

        public int year;
        public DoubleWritable rating;
        public String name;

        public YearRatingNameValue() {
            year = 0;
            rating = new DoubleWritable(0);
            name = null;
        }

    public YearRatingNameValue(int year, double rating, String name) {
            this.year = year;
            this.rating = new DoubleWritable(rating);
            this.name = name;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof YearRatingNameValue)) return false;

            YearRatingNameValue ratYKey = (YearRatingNameValue) o;

            if (year != ratYKey.year) return false;
            if (rating != ratYKey.rating) return false;
            return !(name != null ? !name.equals(ratYKey.name) : ratYKey.name != null);

        }



        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeVInt(dataOutput, year);
            rating.write(dataOutput);
            WritableUtils.writeString(dataOutput, name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            year = WritableUtils.readVInt(dataInput);
            rating.readFields(dataInput);
            name = WritableUtils.readString(dataInput);
        }

        @Override
        public int compareTo(YearRatingNameValue o) {
            if (!name.equals(o.name)) {
                return name.compareTo(o.name);
            } else {
                return Integer.compare(year, o.year) == 0 ?
                        Double.compare(rating.get(), o.rating.get()) : 0;
            }
        }
    }
}
