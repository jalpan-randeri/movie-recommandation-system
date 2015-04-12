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
import java.util.List;
import java.util.NavigableMap;

import com.opencsv.CSVParser;
import conts.MovieConts;
import conts.TableConts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.HBaseConfiguration;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

public class KMeansUserClustering {

	public static class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {

		private HConnection mConnection;
		private HTableInterface mTable;
		private CSVParser mParser = new CSVParser();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			mConnection =  HConnectionManager.createConnection(context.getConfiguration());
			mTable = mConnection.getTable(TableConts.TABLE_NAME.getBytes());
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = mParser.parseLine(value.toString());
			if(tokens.length == 4){
				String user_id = tokens[MovieConts.INDEX_CUST_ID];
				Get query = new Get(Bytes.toBytes(user_id));
				query.setMaxVersions(1);
				Result row = mTable.get(query);

				NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> results = row.getMap();
				System.out.println(results.size());
			}

		}
	}

	public static class KmeansReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if(otherArgs.length != 2){
			System.out.println("Usage KMeansUserClustering <input> <output>");
			System.exit(0);
		}


		boolean isConverged = false;
		int itr = 0;
//		while(!isConverged && itr < 10){
			Job job = new Job(conf, "Kmeans-itration"+itr);
			job.setJarByClass(KMeansUserClustering.class);

			job.setMapperClass(KmeansMapper.class);
			job.setReducerClass(KmeansReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			job.waitForCompletion(true);

			itr++;

			// check for convergence
//		}
	}
}
