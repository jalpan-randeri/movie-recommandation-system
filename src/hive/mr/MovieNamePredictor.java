package hive.mr;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import rjoin.hdfs.Replicated;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * This will read the input file
 * user_id, movie_id
 *
 * and convert into
 *
 * user_id, List[movie_name]
 *
 * Created by jalpanranderi on 4/24/15.
 */
public class MovieNamePredictor {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage MoiveNamePredictor <Distributed Cache> <Input-MoviePredictor> <Output>");
            System.exit(1);
        }

        //  0 - Distributed Cache
        //  1 - Input file
        //  2 - Output directory
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);

        Job job = new Job(conf, "Movie Name Predictor");
        job.setJarByClass(MovieNamePredictor.class);
        job.setNumReduceTasks(10);
        job.setMapperClass(MoiveNameMapper.class);
        job.setReducerClass(MovieNameReducer.class);
        job.setCombinerClass(MovieNameReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class MoiveNameMapper extends Mapper<LongWritable, Text, Text, Text> {

        private CSVParser mParser = new CSVParser();
        private HashMap<String, String> mCachedNames = new HashMap<>();

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
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = mParser.parseLine(value.toString());
            String movie_name = mCachedNames.get(tokens[1]);
            context.write(new Text(tokens[0]), new Text(movie_name));
        }

    }


    public static class MovieNameReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for(Text m : values){
                builder.append(m.toString());
                builder.append(DatasetConts.SEPARATOR);
            }
            builder.deleteCharAt(builder.length() - 1);

            context.write(key, new Text(builder.toString()));
        }
    }

}
