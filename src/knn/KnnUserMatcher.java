package knn;

import java.io.*;

import java.util.*;

import com.opencsv.CSVParser;
import conts.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.DistanceUtils;


/**

/**
 * Created by jalpanranderi on 4/11/15.
 */
public class KnnUserMatcher {

    public static final int K = 25;

    public static class UserData{
        public PriorityQueue<UserInfo> queue = new PriorityQueue<>(K, Collections.reverseOrder());

        public void insert(double distance, KeyUserDistance data, String flag){
            if(queue.size() != K){
                queue.add(new UserInfo(distance, data, flag));
            }else{
                queue.remove();
                queue.add(new UserInfo(distance, data, flag));
            }
        }
    }

    public static class UserInfo implements Comparator<UserInfo>, Comparable<UserInfo>{
        public double distance;
        public KeyUserDistance data;
        public String flag;

        public UserInfo(double distance, KeyUserDistance data, String flag) {
            this.distance = distance;
            this.data = data;
            this.flag = flag;
        }


        @Override
        public int compare(UserInfo o1, UserInfo o2) {
            return Double.compare(o1.distance, o2.distance);
        }

        @Override
        public int compareTo(UserInfo o) {
            return Double.compare(distance, o.distance);
        }
    }


    public static class KnnMapper extends TableMapper<KeyUserDistance, Text> {

        private HashMap<String, AvgReleaseWatch> mCached;
        private CSVParser mParser = new CSVParser();
        private HashMap<String, UserData> mEmmitData = new HashMap<>();

        protected void setup(Context context) throws IOException,
                InterruptedException {
            mCached = new HashMap<>();
            Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (cacheFile != null && cacheFile.length > 0) {
                readFile(cacheFile[0].toString());
            }
        }


        /**
         * read file reads the file which is distributed and added into the HashMap
         * this is comment
         * @param path input file path
         */
        private void readFile(String path) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = mParser.parseLineMulti(line);
                if(tokens.length == 4) {
                    double avg_watch_year = Double.parseDouble(tokens[MovieConts.INDEX_T_WATCH_YEAR]);
                    double avg_release_year = Double.parseDouble(tokens[MovieConts.INDEX_T_RELEASE_YEAR]);
                    String movies = tokens[MovieConts.INDEX_T_MOVIES];
                    mCached.put(tokens[MovieConts.INDEX_T_USR_ID], new AvgReleaseWatch(avg_release_year, avg_watch_year, movies));
                }

            }
            reader.close();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {


            // 1. read the current row
            KeyValue keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_AVG_RELEASE_YEAR.getBytes());
            double avg_release_year = Double.parseDouble(Bytes.toString(keyValue.getValue()));


            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR.getBytes());
            double avg_watched_year = Double.parseDouble(Bytes.toString(keyValue.getValue()));


            keyValue = value.getColumnLatest(TableConts.FAMILY_TBL_DATASET.getBytes(),
                    TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());
            String membership = Bytes.toString(keyValue.getValue());

            // 2. calculate the distance form the all test users
            for (String user : mCached.keySet()) {
                AvgReleaseWatch data = mCached.get(user);
                double dist = DistanceUtils.getEuclideanDistance(avg_release_year, avg_watched_year,
                        data.release_year.get(), data.watch_year.get());

                KeyUserDistance emmit_key = new KeyUserDistance(user, dist, data.watch_year, data.release_year, data.movies);

                // 3. save the data to emmit in the cleanup phase
                if(mEmmitData.containsKey(user)){
                    UserData u = mEmmitData.get(user);
                    u.insert(dist, emmit_key, membership);
                }else{
                    UserData u = new UserData();
                    u.insert(dist, emmit_key, membership);
                    mEmmitData.put(user, u);
                }
                // 3. emmit the (id, dist), user
//                context.write(emmit_key, new Text(membership));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(UserData u : mEmmitData.values()){
                PriorityQueue<UserInfo> queue = u.queue;
                while(!queue.isEmpty()) {
                    UserInfo info = queue.remove();
                    context.write(info.data, new Text(info.flag));
                }

            }
        }
    }

    public static class KnnPartitioner extends Partitioner<KeyUserDistance, Text> {

        @Override
        public int getPartition(KeyUserDistance key, Text value, int numReduceTasks) {
            return (key.user.hashCode() * 127) % numReduceTasks;
        }
    }


    public static class KnnReducer extends Reducer<KeyUserDistance, Text, Text, Text> {
        private HTableInterface mDataset;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mDataset = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME_KNN);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mDataset.close();
        }

        @Override
        protected void reduce(KeyUserDistance key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 1. get the top k
            Iterator<Text> iterator = values.iterator();
            int i = 0;
            List<String> neighbours = new ArrayList<>();

            while(iterator.hasNext() && i < K){
                neighbours.add(iterator.next().toString());
                i++;
            }

            // 2. find the best match
            String tag = getMostOccuring(neighbours);

//            // 3. emmit the result
//            context.write(new Text(key.user), new Text(tag));
            // 4 insert into Hbase

            Put row = new Put(key.user.getBytes());
            row.add(TableConts.FAMILY_TBL_KNN.getBytes(),
                    TableConts.COL_TBL_KNN_MEMBERSHIP.getBytes(),
                    tag.getBytes());
            row.add(TableConts.FAMILY_TBL_KNN.getBytes(),
                    TableConts.COL_TBL_KNN_AVG_RELEASE_YEAR.getBytes(),
                    String.valueOf(key.releaseYear.get()).getBytes());
            row.add(TableConts.FAMILY_TBL_KNN.getBytes(),
                    TableConts.COL_TBL_KNN_AVG_WATCHED_YEAR.getBytes(),
                    String.valueOf(key.watchYear.get()).getBytes());
            row.add(TableConts.FAMILY_TBL_KNN.getBytes(),
                    TableConts.COL_TBL_KNN_MOVIE_LIST.getBytes(),
                    key.movies.getBytes());
            mDataset.put(row);

        }


        /**
         * get the most occurring flag form the neighbour to find the
         * best matching neighbour
         * @param neighbours List[Flags]
         * @return String Flag as the member of cluster
         */
        private String getMostOccuring(List<String> neighbours) {
            HashMap<String, Integer> maxK = new HashMap<>();
            for (String k : neighbours) {
                if (maxK.containsKey(k)) {
                    int countK = maxK.get(k);
                    countK++;
                    maxK.put(k, countK);
                } else {
                    maxK.put(k, 1);
                }
            }

            String max = "NULL";
            int maxKCount = 0;
            for (String key : maxK.keySet()) {
                int currentCount = maxK.get(key);
                if (maxKCount < currentCount) {
                    maxKCount = currentCount;
                    max = key;
                }
            }
            return max;
        }
    }

    public static class AvgReleaseWatch implements WritableComparable<AvgReleaseWatch>{

        public DoubleWritable release_year;
        public DoubleWritable watch_year;
        public String movies;

        public AvgReleaseWatch() {
            release_year = new DoubleWritable();
            watch_year = new DoubleWritable();
            movies = "";
        }

        public AvgReleaseWatch(double realse_year, double watch_year, String movies) {
            this.release_year = new DoubleWritable(realse_year);
            this.watch_year = new DoubleWritable(watch_year);
            this.movies = movies;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AvgReleaseWatch that = (AvgReleaseWatch) o;

            if (release_year != null ? !release_year.equals(that.release_year) : that.release_year != null) return false;
            if (watch_year != null ? !watch_year.equals(that.watch_year) : that.watch_year != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = release_year != null ? release_year.hashCode() : 0;
            result = 31 * result + (watch_year != null ? watch_year.hashCode() : 0);
            return result;
        }

        @Override
        public int compareTo(AvgReleaseWatch o) {
            return release_year.compareTo(o.release_year) == 0 ?
                    watch_year.compareTo(o.watch_year) :
                    release_year.compareTo(o.release_year);

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            release_year.write(dataOutput);
            watch_year.write(dataOutput);
            WritableUtils.writeString(dataOutput, movies);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            release_year.readFields(dataInput);
            watch_year.readFields(dataInput);
            movies = WritableUtils.readString(dataInput);
        }
    }

    public static class KeyUserDistance implements WritableComparable<KeyUserDistance>{

        public String user;
        public DoubleWritable distance;
        public DoubleWritable watchYear;
        public DoubleWritable releaseYear;
        public String movies;

        public KeyUserDistance() {
            user = "";
            distance = new DoubleWritable(0);
            watchYear = new DoubleWritable(0);
            releaseYear = new DoubleWritable(0);
            movies = "";
        }

        public KeyUserDistance(String user, double distance, DoubleWritable watchYear, DoubleWritable releaseYear, String movies) {
            this.user = user;
            this.distance = new DoubleWritable(distance);
            this.watchYear = watchYear;
            this.releaseYear = releaseYear;
            this.movies = movies;
        }

        public String getUser() {
            return user;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyUserDistance that = (KeyUserDistance) o;

            if (!distance.equals(that.distance)) return false;
            if (!user.equals(that.user)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = user.hashCode();
            result = 31 * result + distance.hashCode();
            return result;
        }


        @Override
        public int compareTo(KeyUserDistance o) {
            int result =  user.compareTo(o.user);
            if (result == 0) {
                return  distance.compareTo(o.distance);
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput, user);
            distance.write(dataOutput);
            watchYear.write(dataOutput);
            releaseYear.write(dataOutput);
            WritableUtils.writeString(dataOutput, movies);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            user = WritableUtils.readString(dataInput);
            distance.readFields(dataInput);
            watchYear.readFields(dataInput);
            releaseYear.readFields(dataInput);
            movies = WritableUtils.readString(dataInput);
        }
    }



    public static class KeySortingComparator extends WritableComparator{

        protected KeySortingComparator() {
            super(KeyUserDistance.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyUserDistance k1 = (KeyUserDistance) a;
            KeyUserDistance k2 = (KeyUserDistance) b;

            if(k1.user.equals(k2.user)){
                return k1.distance.compareTo(k2.distance);
            }else{
                return k1.user.compareTo(k2.user);
            }
        }
    }

    public static class KeyGrouppingComparator extends WritableComparator{

        protected KeyGrouppingComparator() {
            super(KeyUserDistance.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyUserDistance k1 = (KeyUserDistance) a;
            KeyUserDistance k2 = (KeyUserDistance) b;

            return k1.user.compareTo(k2.user);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: KNN <test.csv>");
            System.exit(2);
        }

        createTable(conf);
        // - 0 Distributed Cache file
        // - 1 Output directory
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);

        Job job = new Job(conf, "Knn");
        job.setJarByClass(KnnUserMatcher.class);

        job.setMapperClass(KnnMapper.class);
        job.setReducerClass(KnnReducer.class);

        job.setGroupingComparatorClass(KeyGrouppingComparator.class);
        job.setSortComparatorClass(KeySortingComparator.class);
        job.setPartitionerClass(KnnPartitioner.class);
        job.setNumReduceTasks(K);


        job.setOutputKeyClass(KeyUserDistance.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_KNN);

        Scan scan = new Scan();
        scan.addFamily(TableConts.FAMILY_TBL_DATASET.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_AVG_RELEASE_YEAR.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_AVG_WATCHED_YEAR.getBytes());
        scan.addColumn(TableConts.FAMILY_TBL_DATASET.getBytes(), TableConts.COL_TBL_DATASET_MEMBERSHIP.getBytes());
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_DATASET,
                scan,
                KnnMapper.class,
                KeyUserDistance.class,
                Text.class,
                job);
        job.waitForCompletion(true);


    }

    private static void createTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        // main centroids locations
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_KNN);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_TBL_KNN));
        if (admin.tableExists(TableConts.TABLE_NAME_KNN)) {
            admin.disableTable(TableConts.TABLE_NAME_KNN);
            admin.deleteTable(TableConts.TABLE_NAME_KNN);
        }
        admin.createTable(hd);
        admin.close();
    }
}
