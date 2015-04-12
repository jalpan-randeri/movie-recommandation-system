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
import kmeans.mappers.KMeansMapper;
import kmeans.reducers.KMeansReducer;
import kmeans.utils.CentroidUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.out.println("Usage KMeansUserClustering <input> <output>");
            System.exit(0);
        }

        initCentroidTable(conf);
        setupConnectionToTables(conf);


        boolean isConverged = false;
        int itr = 0;


        String[] centroids = {"885013", "2442", "814701"};
        initalizePreviousCentroidTable(KMeansConts.K, centroids);

        while (!isConverged && itr < 10) {
            Job job = new Job(conf, "Kmeans-itration" + itr);
            job.setJarByClass(KMeansUserClustering.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setNumReduceTasks(KMeansConts.K);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            job.waitForCompletion(true);

            itr++;

            // check for convergence
            isConverged = getConvergence(KMeansConts.K);
        }
        cleanupTables();
    }

    private static void initalizePreviousCentroidTable(int k, String[] centroids) throws IOException {
        for (int i = 0; i < k; i++) {
            Put row = new Put(Bytes.toBytes(i));
            row.add(TableConts.TABLE_CENTROID_FAMAILY.getBytes(),
                    TableConts.TABLE_CENTROID_COLUMN_ID_CENTROID.getBytes(), centroids[i].getBytes());

            mCentroidTable.put(row);

        }
    }

    private static void setupConnectionToTables(Configuration conf) throws IOException {
        mConnection = HConnectionManager.createConnection(conf);
        mCentroidTable = mConnection.getTable(TableConts.TABLE_NAME_CENTROID.getBytes());
        mNewCentroidTable = mConnection.getTable(TableConts.TABLE_NAME_NEW_CENTROID.getBytes());
    }

    private static void cleanupTables() throws IOException {
        mNewCentroidTable.close();
        mCentroidTable.close();
        mConnection.close();
    }

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
                int i1 = Integer.parseInt(o1);
                int i2 = Integer.parseInt(o2);

                return i1 - i2;
            }
        });


        // check if both list are common or not
        return isSame(prev_centroids, new_centroids);
    }

    private static boolean isSame(List<String> prev_centroids, List<String> new_centroids) {
        for (int i = 0; i < prev_centroids.size(); i++) {
            if (!prev_centroids.get(i).equals(new_centroids.get(i))) {
                return false;
            }
        }

        return true;
    }


    private static void initCentroidTable(Configuration conf) throws IOException {
        Configuration co = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(co);

        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME_CENTROID);
        hd.addFamily(new HColumnDescriptor(TableConts.TABLE_CENTROID_FAMAILY));
        // main centroids locations
        if (admin.tableExists(TableConts.TABLE_NAME_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_CENTROID);
        }
        admin.createTable(hd);

        HTableDescriptor hd1 = new HTableDescriptor(TableConts.TABLE_NAME_NEW_CENTROID);
        hd1.addFamily(new HColumnDescriptor(TableConts.TABLE_NEW_CENTROID_FAMAILY));
        // new centroid location
        if (admin.tableExists(TableConts.TABLE_NAME_NEW_CENTROID)) {
            admin.disableTable(TableConts.TABLE_NAME_NEW_CENTROID);
            admin.deleteTable(TableConts.TABLE_NAME_CENTROID);
        }
        admin.createTable(hd1);


        admin.close();
    }
}
