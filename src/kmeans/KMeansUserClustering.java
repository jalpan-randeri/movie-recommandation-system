/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package kmeans;

import java.io.IOException;

import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class KMeansUserClustering {

	public static class kMeansMapper extends Mapper<Object, Text, Text, Text> {

		private String centroid1;
		private String centroid2;
		private String centroid3;
		private String centroid4;
		private String centroid5;
		private String flag;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();

			centroid1 = conf.get("c1");
			centroid2 = conf.get("c2");
			centroid3 = conf.get("c3");
			centroid4 = conf.get("c4");
			centroid5 = conf.get("c5");
			flag = conf.get("flag");

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, TableConts.TABLE_NAME);

			Scan s = new Scan();
			// s.addColumn(Bytes.toBytes(TableConts.TABLE_USR_MOV_COL_FAMILY),
			// Bytes.toBytes(TableConts.TABLE_USR_MOV_COLUMN_USR_ID));
			ResultScanner scanner = table.getScanner(s);
			//System.out.println("Found row: " + scanner.next());
			try {
				Result result = null;
				while ((result = scanner.next()) != null) {
					System.out.println(result);
					byte[] userId = result.getValue(TableConts.TABLE_USR_MOV_COL_FAMILY.getBytes(), TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV.getBytes());
					//String id = userId.toString();
					System.out.println(userId.toString());
				}

			} finally {
				scanner.close();
				table.close();
			}

			System.out.println(centroid1);

		}
	}

	public static class kMeansReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();


		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: KMeansUserClustering <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Kmeans User Clustering");

		job.setNumReduceTasks(5);
		job.setJarByClass(KMeansUserClustering.class);
		job.setMapperClass(kMeansMapper.class);
		job.setReducerClass(kMeansReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		conf.setInt("c1", 1488844);
		conf.setInt("c2", 822109);
		conf.setInt("c3", 885013);
		conf.setInt("c4", 30878);
		conf.setInt("c5", 823519);
		conf.setBoolean("flag", false);
		job.waitForCompletion(true);

		System.exit(0);
	}
}
