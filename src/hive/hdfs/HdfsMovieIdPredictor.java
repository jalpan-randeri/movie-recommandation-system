package hive.hdfs;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by jalpanranderi on 4/25/15.
 */
public class HdfsMovieIdPredictor {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 3){
            System.out.println("Usage : HdfsMoviePredictor movie_titles.txt output/mapClusterMoive output/moivePrediction");
            System.exit(1);
        }

        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);

        Job job = new Job(conf, "HDFS Cluster Movie");
        job.setJarByClass(HdfsMovieIdPredictor.class);
        job.setMapperClass(MidMapper.class);
        job.setReducerClass(MidReducer.class);
        job.setCombinerClass(MidReducer.class);

        job.setNumReduceTasks(10);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));



        job.waitForCompletion(true);
    }

    public static class MidMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HTable mTestUsers;
        private HashMap<String, String> mCachedClusters = new HashMap<>();
        private HashMap<String, String> mCachedMovies = new HashMap<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mTestUsers = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_KNN);
            mTestUsers.setAutoFlush(false);

            readTable();
        }

        private void readTable() throws IOException {
            // Instantiating the Scan class
            Scan scan = new Scan();
            // Scanning the required columns
            scan.addColumn(TableConts.FAMILY_TBL_KNN.getBytes(), TableConts.COL_TBL_KNN_MOVIE_LIST.getBytes());
            scan.addColumn(TableConts.FAMILY_TBL_KNN.getBytes(), TableConts.COL_TBL_KNN_MEMBERSHIP.getBytes());



            // Getting the scan result
            ResultScanner scanner = mTestUsers.getScanner(scan);

            // Reading values from scan result

            for (Result result : scanner) {

                byte[] cb = result.getRow();
                byte[] c_movies = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_KNN),
                        Bytes.toBytes(TableConts.COL_TBL_KNN_MOVIE_LIST));
                byte[] c_membership = result.getValue(Bytes.toBytes(TableConts.FAMILY_TBL_KNN),
                        Bytes.toBytes(TableConts.COL_TBL_KNN_MEMBERSHIP));


                String id = Bytes.toString(cb);
                String movies = Bytes.toString(c_movies);
                String flag = Bytes.toString(c_membership);

                mCachedClusters.put(id, flag);
                mCachedMovies.put(id, movies);

            }
            //closing the scanner
            scanner.close();
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(DatasetConts.SEPARATOR);
            boolean isKey = true;
            String cluster_id = null;
            while (tokenizer.hasMoreTokens()){
                if(isKey){
                    cluster_id = tokenizer.nextToken();
                    isKey=false;
                }else{
                    String movie = tokenizer.nextToken();
                    for(String test_user : mCachedClusters.keySet()) {
                        if (mCachedClusters.get(test_user).equals(cluster_id)) {
                            String[] watched_movies = mCachedMovies.get(test_user).split(DatasetConts.SEPARATOR);
                            if (Arrays.binarySearch(watched_movies, movie) == -1) {
                                context.write(new Text(test_user), new Text(movie));
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTestUsers.close();
        }
    }

    public static class MidReducer extends Reducer<Text, Text, Text, Text> {

        private CSVParser mParser = new CSVParser();
        private HashMap<String, String> mCachedNames = new HashMap<>();



        @Override
        public void setup(Context context) throws IOException, InterruptedException {
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
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text t : values){
                String ans = String.format("%s,%s",key.toString(), mCachedNames.get(t.toString()));
                context.write(new Text(), new Text(ans));
            }
        }

    }

}
