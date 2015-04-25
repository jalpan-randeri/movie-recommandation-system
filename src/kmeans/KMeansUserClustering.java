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

import conts.DatasetConts;
import conts.KMeansConts;
import conts.TableConts;
import kmeans.model.Centroid;
import kmeans.mappers.KMeansMapper;
import kmeans.model.EmitValue;
import kmeans.reducers.KMeansReducer;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
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


        String[] centroids = {"1001461,2.0,2003.0", "1001779,2.0,2004.0", "1005769,5.0,2004.0", "1007254,4.0,2005.0", "1007809,4.0,2003.0"};
        initializePreviousCentroidTable(KMeansConts.K, centroids);

        while (!isConverged && itr < 50) {

            clearMembership(conf);

            Job job = new Job(conf, "Kmeans-itration" + itr);
            job.setJarByClass(KMeansUserClustering.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setNumReduceTasks(KMeansConts.K);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(EmitValue.class);

            job.setOutputFormatClass(TableOutputFormat.class);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME_NEW_CENTROID);


            Scan scan = new Scan();
            scan.addFamily(TableConts.FAMILY_TBL_DATASET.getBytes());
            scan.setCaching(500);
            scan.setCacheBlocks(false);
            TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME_DATASET,
                    scan,
                    KMeansMapper.class,
                    IntWritable.class,
                    EmitValue.class,
                    job);
            job.waitForCompletion(true);
            itr++;
            System.out.println("iteration -> " + itr);
            // check for convergence
            isConverged = getConvergence(KMeansConts.K);
            updateCentroids(KMeansConts.K);
        }
        cleanupTables();
    }

    /**
     * remove the membership table
     *
     * @param conf Configuration
     * @throws IOException
     */
    private static void clearMembership(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);


        HTableDescriptor cluster_table = new HTableDescriptor(TableConts.TABLE_NAME_CLUSTERS);
        cluster_table.addFamily(new HColumnDescriptor(TableConts.FAMILY_CLUSTERS));
        if (admin.tableExists(TableConts.TABLE_NAME_CLUSTERS)) {
            admin.disableTable(TableConts.TABLE_NAME_CLUSTERS);
            admin.deleteTable(TableConts.TABLE_NAME_CLUSTERS);
        }
        admin.createTable(cluster_table);

        admin.close();
    }

    /**
     * update centroid will upadte the previous centroid with new centroids
     *
     * @param k Integer k total number of centroids
     * @throws IOException
     */
    private static void updateCentroids(int k) throws IOException {
        List<Centroid> prev_centroids = CentroidUtils.getCentroids(mCentroidTable, k);
        List<Centroid> update_centroid = CentroidUtils.getNewCentroids(mNewCentroidTable, k);

        Centroid[] centroids = new Centroid[update_centroid.size()];
        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = update_centroid.get(i) == null ? prev_centroids.get(i) : update_centroid.get(i);
        }

        resetPreviousCentroidTable(k, centroids);
    }

    private static void resetPreviousCentroidTable(int k, Centroid[] centroids) throws IOException {
        for (int i = 0; i < centroids.length; i++) {

            String x = String.valueOf(centroids[i].rating_x);
            String y = String.valueOf(centroids[i].year_y);

            Put row = new Put(String.valueOf(i).getBytes());
            row.add(TableConts.FAMILY_CENTROID.getBytes(),
                    TableConts.COL_TBL_CENTROID_COL_X.getBytes(), x.getBytes());
            row.add(TableConts.FAMILY_CENTROID.getBytes(),
                    TableConts.COL_TBL_CENTROID_COL_Y.getBytes(), y.getBytes());

            mCentroidTable.put(row);
        }
    }

    /**
     * initalize previous centroids
     *
     * @param k         Integer total number of centroids
     * @param centroids String[] centroids
     * @throws IOException
     */
    private static void initializePreviousCentroidTable(int k, String[] centroids) throws IOException {
        for (int i = 0; i < k; i++) {
            String[] tokens = centroids[i].split(DatasetConts.SEPARATOR);

            Put row = new Put(String.valueOf(i).getBytes());

            row.add(TableConts.FAMILY_CENTROID.getBytes(),
                    TableConts.COL_TBL_CENTROID_COL_X.getBytes(), tokens[1].getBytes());
            row.add(TableConts.FAMILY_CENTROID.getBytes(),
                    TableConts.COL_TBL_CENTROID_COL_Y.getBytes(), tokens[2].getBytes());

            mCentroidTable.put(row);
        }
    }

    /**
     * setup connection to table will generate datasource connections
     *
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
     *
     * @throws IOException
     */
    private static void cleanupTables() throws IOException {
        mNewCentroidTable.close();
        mCentroidTable.close();
        mConnection.close();
    }

    /**
     * determine if the convergence is happen or not
     *
     * @param k Integer representing the total number of clusters
     * @return Boolean true iff convergence happens
     * @throws IOException
     */
    private static boolean getConvergence(int k) throws IOException {
        List<Centroid> prev_centroids = CentroidUtils.getCentroids(mCentroidTable, k);
        List<Centroid> new_centroids = CentroidUtils.getNewCentroids(mNewCentroidTable, k);

        // check if both list are common or not
        return isSame(prev_centroids, new_centroids);
    }

    /**
     * true iff both list are same
     *
     * @param prev_centroids [String]
     * @param new_centroids  [String]
     * @return Boolean
     */
    private static boolean isSame(List<Centroid> prev_centroids, List<Centroid> new_centroids) {
        boolean ans = false;
        for (int i = 0; i < prev_centroids.size(); i++) {
            ans = false;
            Centroid c = prev_centroids.get(i);
            for (int j = 0; j < new_centroids.size(); j++) {
                ans = ans || new_centroids.get(j).equals(c);
            }

            if (!ans) {
                return false;
            }
        }


        System.out.println("Converged");
        return true;
    }

    /**
     * generate the Hbase tables required to hold centroids
     *
     * @param conf Configuration
     * @throws IOException
     */
    private static void initCentroidTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        // main centroids locations
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_CENTROID);
        hd.addFamily(new HColumnDescriptor(TableConts.FAMILY_CENTROID));
        if (admin.tableExists(TableConts.TABLE_NAME_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_CENTROID);
        }
        admin.createTable(hd);


        // new centroid location
        HTableDescriptor hd1 = new HTableDescriptor(TableConts.TABLE_NAME_NEW_CENTROID);
        hd1.addFamily(new HColumnDescriptor(TableConts.FAMILY_NEW_CENTROID));
        if (admin.tableExists(TableConts.TABLE_NAME_NEW_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_NEW_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_NEW_CENTROID);
        }
        admin.createTable(hd1);


        admin.close();
    }
}
