/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kmeans;

import conts.KMeansConts;
import conts.TableConts;
import kmeans.comparator.CentroidGrouppingComparator;
import kmeans.mappers.KMeansMapper;
import kmeans.reducers.KMeansReducer;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class KMeansUserClustering {

    private static HConnection mConnection;
    private static HTableInterface mCentroidTable;
    private static HTableInterface mNewCentroidTable;

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();




        initCentroidTable(conf);
        setupConnectionToTables(conf);


        boolean isConverged = false;
        int itr = 0;


        String[] centroids = {"1488844", "822109", "885013", "30878", "823519","1711859", "2263586", "4326", "769643", "850327"};
        initializePreviousCentroidTable(KMeansConts.K, centroids);

        while (!isConverged && itr < 500) {

            clearMembership(conf);

            Job job = new Job(conf, "Kmeans-itration" + itr);
            job.setJarByClass(KMeansUserClustering.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setNumReduceTasks(KMeansConts.K);
            job.setGroupingComparatorClass(CentroidGrouppingComparator.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(TableOutputFormat.class);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_NEW_CENTROID);


            Scan scan = new Scan();
            scan.setCaching(500);
            scan.setCacheBlocks(false);
            TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME, scan, KMeansMapper.class, Text.class, Text.class, job);



            job.waitForCompletion(true);

            itr++;
            System.out.println("iteration -> "+itr);
            // check for convergence
            isConverged = getConvergence(KMeansConts.K);
            updateCentroids(KMeansConts.K);
        }
        cleanupTables();
    }

    /**
     * remove the membership table
     * @param conf Configuration
     * @throws IOException
     */
    private static void clearMembership(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);


        HTableDescriptor cluster_table = new HTableDescriptor(TableConts.TABLE_NAME_CLUSTERS);
        cluster_table.addFamily(new HColumnDescriptor(TableConts.TABLE_CLUSTERS_FAMILY));
        if(admin.tableExists(TableConts.TABLE_NAME_CLUSTERS)){
            admin.disableTable(TableConts.TABLE_NAME_CLUSTERS);
            admin.deleteTable(TableConts.TABLE_NAME_CLUSTERS);
        }
        admin.createTable(cluster_table);

        admin.close();
    }

    /**
     * update centroid will upadte the previous centroid with new centroids
     * @param k Integer k total number of centroids
     * @throws IOException
     */
    private static void updateCentroids(int k) throws IOException {
        List<String> prev_centroids = CentroidUtils.getCentroids(mCentroidTable, k);
        List<String> update_centroid = CentroidUtils.getNewCentroids(mNewCentroidTable, k);

        String[] centroid = new String[update_centroid.size()];
        for(int i = 0; i < centroid.length; i++){
            centroid[i] = update_centroid.get(i) == null ? prev_centroids.get(i) : update_centroid.get(i);
        }

        initializePreviousCentroidTable(k, centroid);
    }

    /**
     * initalize previous centroids
     * @param k Integer total number of centroids
     * @param centroids String[] centroids
     * @throws IOException
     */
    private static void initializePreviousCentroidTable(int k, String[] centroids) throws IOException {
        for (int i = 0; i < k; i++) {
            Put row = new Put(String.valueOf(i).getBytes());
            row.add(TableConts.TABLE_CENTROID_FAMAILY.getBytes(),
                    TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID.getBytes(), centroids[i].getBytes());

            mCentroidTable.put(row);
        }
    }

    /**
     * setup connection to table will generate datasource connections
     * @param conf Configuration
     * @throws IOException
     */
    private static void setupConnectionToTables(Configuration conf) throws IOException {
        mConnection = HConnectionManager.createConnection(conf);
        mCentroidTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());
        mNewCentroidTable = mConnection.getTable(TableConts.TABLE_NAME_NEW_CENTROID.getBytes());
    }

    /**
     * clean up table will close all the connection from hbase
     * @throws IOException
     */
    private static void cleanupTables() throws IOException {
        mNewCentroidTable.close();
        mCentroidTable.close();
        mConnection.close();
    }

    /**
     * determine if the convergence is happen or not
     * @param k Integer representing the total number of clusters
     * @return Boolean true iff convergence happens
     * @throws IOException
     */
    private static boolean getConvergence(int k) throws IOException {
        List<String> prev_centroids = CentroidUtils.getCentroids(mCentroidTable, k);
        List<String> new_centroids = CentroidUtils.getNewCentroids(mNewCentroidTable, k);

        // sort both lists
        Collections.sort(prev_centroids, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int i1 = Integer.parseInt(o1);
                int i2 = Integer.parseInt(o2);

                return i1 - i2;
            }
        });


        Collections.sort(new_centroids, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1 != null && o2 != null) {
                    int i1 = Integer.parseInt(o1);
                    int i2 = Integer.parseInt(o2);

                    return i1 - i2;
                }else{
                    return  -1;
                }
            }
        });


        // check if both list are common or not
        return isSame(prev_centroids, new_centroids);
    }

    /**
     * true iff both list are same
     * @param prev_centroids [String]
     * @param new_centroids [String]
     * @return Boolean
     */
    private static boolean isSame(List<String> prev_centroids, List<String> new_centroids) {
        for (int i = 0; i < prev_centroids.size(); i++) {
            if (!prev_centroids.get(i).equals(new_centroids.get(i))) {
                return false;
            }
        }
        System.out.println("Converged");
        return true;
    }

    /**
     * generate the Hbase tables required to hold centroids
     * @param conf Configuration
     * @throws IOException
     */
    private static void initCentroidTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        // main centroids locations
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_CENTROID);
        hd.addFamily(new HColumnDescriptor(TableConts.TABLE_CENTROID_FAMAILY));
        if (admin.tableExists(TableConts.TABLE_NAME_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_CENTROID);
        }
        admin.createTable(hd);


        // new centroid location
        HTableDescriptor hd1 = new HTableDescriptor(TableConts.TABLE_NAME_NEW_CENTROID);
        hd1.addFamily(new HColumnDescriptor(TableConts.TABLE_NEW_CENTROID_FAMAILY));
        if (admin.tableExists(TableConts.TABLE_NAME_NEW_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_NEW_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_NEW_CENTROID);
        }
        admin.createTable(hd1);



        admin.close();
    }
}
