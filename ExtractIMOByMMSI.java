
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class ExtractIMOByMMSI {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		private static HashSet<String> keyWord;
		private Path[] localFiles;

		@SuppressWarnings("deprecation")
		public void setup(Context context) throws IOException, InterruptedException {

			keyWord = new HashSet<String>();
			localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (int i = 0; i < localFiles.length; i++) {
				String a;
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((a = br.readLine()) != null) {
					keyWord.add(a);
				}
				br.close();
			}

		}

		@Override
		protected void map(Object k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] lonlat = v1.toString().split(",");
			if (lonlat.length > 26) {
				String utc = lonlat[0];
				String Mmsi = lonlat[1];
				String status= lonlat[2];
				Double lon1 = Double.parseDouble(lonlat[6]);
				Double lat1 = Double.parseDouble(lonlat[7]);
				String lon = lonlat[6];
				String lat = lonlat[7];
				String cog= lonlat[8];
				String true_head =lonlat[9];
				String length =lonlat[16];
				String width =lonlat[17];
//				String lonlat2 = String.valueOf(lon) + "," + String.valueOf(lat);
                int   sta =Integer.parseInt(status);
				String utclonlat = utc + "," + Mmsi +","+status+"," +lon+","+lat+","+cog+","+true_head+","+length+","+width;
				// 渤海区域,船只类型为IMO的数据
				if (keyWord.contains(Mmsi)) {
//					if (117.23 < lon1 && lon1 < 124.26 && 36.32 < lat1 && lat1 < 41.10 && sta ==5) {
//						context.write(new Text(Mmsi), new Text(utclonlat));
//					}
//					新加坡区域的系泊数据
					if (103.115 < lon1 && lon1 < 104.993 && 0.433387 < lat1 && lat1 < 1.75325 && sta ==5) {
						context.write(new Text(Mmsi), new Text(utclonlat));
					}
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> mos;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				mos.write(NullWritable.get(), value, key.toString());// (key,value,filename)
			}
		}

		@Override
		protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			mos.close();
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("hdfs://192.168.10.32:9000/hadoop/data/common/zzt/MmsiExt.csv"), conf);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "6MonthIMOxibo");
		job.setJarByClass(ExtractIMOByMMSI.class);

		job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024);
		job.setNumReduceTasks(43);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

//		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/DSLinked_ByMonth/04/";
		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/DSLinked_ByMonth/06/";
		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMO/6MonthIMOxibo-singapore/";

//		SequenceFileInputFormat.addInputPath(job, new Path(input1));
		SequenceFileInputFormat.addInputPath(job, new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
