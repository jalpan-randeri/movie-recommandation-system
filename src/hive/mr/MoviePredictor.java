package hive.mr;

import com.opencsv.CSVParser;
import conts.DatasetConts;
import conts.KMeansConts;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

/**
 *
 * This table will generate the
 *
 * Created by jalpanranderi on 4/24/15.
 */
public class MoviePredictor {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        if(otherArgs.length != 2){
            System.out.println("Usage: Movie Predictor movie_title.txt <output>");
            System.exit(1);
        }


        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);


        Job job = new Job(conf, "Movie Predictor");
        job.setJarByClass(MoviePredictor.class);

        job.setMapperClass(PredictorMapper.class);
        job.setReducerClass(PredictorReducer.class);

        job.setNumReduceTasks(KMeansConts.K);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        Scan scan = new Scan();
        scan.addFamily(TableConts.FAMILY_TBL_CLUSTER_MOVIES.getBytes());
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_CLUSTER_MOVIES,
                scan,
                PredictorMapper.class,
                Text.class,
                Text.class,
                job);
        job.waitForCompletion(true);

    }

    public static class PredictorMapper extends TableMapper<Text, Text>{
        HTable mTestUsers;
        HashMap<String, String> mCachedMovies = new HashMap<>();
        HashMap<String, String> mCachedClusters = new HashMap<>();

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
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String row_key = Bytes.toString(key.get());
            String[] tokens = row_key.split(DatasetConts.SEPARATOR);

            String cluster_id = tokens[0];
            String movie = tokens[1];

            for(String test_user : mCachedClusters.keySet()){
                if(mCachedClusters.get(test_user).equals(cluster_id)){
                    String[] watched_movies = mCachedMovies.get(test_user).split(DatasetConts.SEPARATOR);
                    if(Arrays.binarySearch(watched_movies, movie) == -1){
                        context.write(new Text(test_user), new Text(movie));
                    }
                }
            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mTestUsers.close();
        }
    }


    public static class PredictorReducer extends Reducer<Text, Text, Text, Text>{

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
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for(Text t : values){
                i++;
                String ans = String.format("%s,%s",key.toString(), mCachedNames.get(t.toString()));
                context.write(new Text(), new Text(ans));
                if(i == 10){
                    break;
                }
            }


        }
    }

}
