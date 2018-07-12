package com.ncut.marineTrafic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Combine {

	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.print("Before Mapper: " + key + ", " + value);
			String[] values = value.toString().split(",");
			String lon = values[0];
			String lat = values[1];
			int num = Integer.parseInt(values[2]);
			String lonlat = lon + "," + lat;
			context.write(new Text(lonlat), new IntWritable(num));
		}
	}

	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}			
			key.set(new Text(key) + "," + sum);
			context.write(key, null);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Combine.class);
		job.setJobName("fullYearAllships20bitsGrid");

		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanmaobo/allships4-5-6-20bitsgrid/";
		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanmaobo/allships4-5-6-20bitsgrid/";

		
		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanmaobo/fullYearAllships20bitsGrid/";

//		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/GridCombine/7_8_9_10_11_12MonthGridCombine/";
//		String input3 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMOPOIEXTGrid/06MonthIMOPOI20bitsGrided/";
//		String input4 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMOPOIEXTGrid/10MonthIMOPOI24bitsGrided/";
//		String input5 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMOPOIEXTGrid/11MonthIMOPOI24bitsGrided/";
//		String input6 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMOPOIEXTGrid/12MonthIMOPOI24bitsGrided/";

//		String input4 = "hdfs://192.168.10.32:9000/hadoop/data/common/lizhuoran/IMO-BIT11-MergedFiles/05IMOtotal";
//		String input5 = "hdfs://192.168.10.32:9000/hadoop/data/common/lizhuoran/IMO-BIT11-MergedFiles/06IMOtotal";
//		String input6 = "hdfs://192.168.10.32:9000/hadoop/data/common/lizhuoran/IMO-BIT11-MergedFiles/07IMOtotal";
		


		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.addInputPath(job, new Path(input2));
//		FileInputFormat.addInputPath(job, new Path(input3));
//		FileInputFormat.addInputPath(job, new Path(input4));
//		FileInputFormat.addInputPath(job, new Path(input5));

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		System.out.println("Finished!!!!!!");
	}
}