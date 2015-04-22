package hbase_populate.mappers;

import com.opencsv.CSVParser;
import conts.MovieConts;
import hbase_populate.model.MovKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jalpanranderi on 4/16/15.
 */
public class HMapper extends Mapper<LongWritable, Text, MovKey, IntWritable> {

    private CSVParser parser = new CSVParser();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = parser.parseLine(value.toString());

        if(tokens.length == 4) {
            MovKey mKey = new MovKey(tokens[MovieConts.INDEX_CUST_ID], tokens[MovieConts.INDEX_MOVIE_ID]);
            context.write(mKey, new IntWritable(Integer.parseInt(tokens[MovieConts.INDEX_RATING])));
        }
    }

}
