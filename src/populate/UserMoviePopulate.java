package populate;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 *
 * User Movie Populate will generate the text file
 * this is modification of dataset
 * (Movie_id, user_id, watch_rating, year)  => (user_id, List[movie_id, watch_rating])
 *
 * Created by jalpanranderi on 4/12/15.
 */
public class UserMoviePopulate {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 2) {
            System.err.println("Usage: UserMoviePopulate <input> <output>");
            System.exit(1);
        }


        Job job = new Job(conf, "Populate Data into HDFS");
        job.setJarByClass(UserMoviePopulate.class);

        job.setCombinerClass(DatasetReducer.class);

        job.setMapperClass(DatasetMapper.class);
        job.setReducerClass(DatasetReducer.class);
        job.setNumReduceTasks(10);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




    public static class DatasetMapper extends Mapper<LongWritable, Text, Text, Text>{
        private CSVParser parse = new CSVParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = parse.parseLine(value.toString());

            if(tokens.length == 4){

                String out = String.format("%s,%s", tokens[MovieConts.INDEX_MOVIE_ID], tokens[MovieConts.INDEX_RATING]);
                context.write(new Text(tokens[MovieConts.INDEX_CUST_ID]),
                              new Text(out));
            }
        }
    }

    public static class DatasetReducer extends Reducer<Text, Text, Text, Text>{

        private StringBuilder builder = new StringBuilder();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text v : values){
                builder.append(v.toString());
                builder.append(DatasetConts.SEPARATOR);
            }

            context.write(key, new Text(builder.toString()));

            builder.setLength(0);
        }
    }

}
