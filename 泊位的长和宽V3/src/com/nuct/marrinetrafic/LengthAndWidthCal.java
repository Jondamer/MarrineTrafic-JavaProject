package com.nuct.marrinetrafic;

/**
 * 根据泊位中心点的数据，求取该泊位的长和宽
 * @author Administrator
 *
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

public class LengthAndWidthCal {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		private Path[] localFiles;
		private static Coordinate[] coordinates = new Coordinate[154];
		private static GeometryFactory geometryFactory = new GeometryFactory();
		private static MultiPoint mp;

		@SuppressWarnings("deprecation")
		public void setup(Context context) throws IOException, InterruptedException {
			localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (int i = 0; i < localFiles.length; i++) {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				int j = 0;
				while ((line = br.readLine()) != null) {
					String[] lonlat = line.split(",");
					double lon = Double.parseDouble(lonlat[0]);
					double lat = Double.parseDouble(lonlat[1]);
					Coordinate coord = new Coordinate(lon, lat);
					coordinates[j] = coord;
					j++;
				}
				br.close();
			}
			// mp = geometryFactory.createMultiPoint(coordinates);
		}

		@Override
		protected void map(Object k1, Text v1, Context context) throws IOException, InterruptedException {
			 String[] lonlat = v1.toString().split(",");
			 if (lonlat.length > 26) {
			 String Mmsi = lonlat[1];
			 Double lon = Double.parseDouble(lonlat[6]);
			 Double lat = Double.parseDouble(lonlat[7]);
			 String lonlat1 = String.valueOf(lon) + "," + String.valueOf(lat);
			 String shiplength = lonlat[16];
			 String shipwidth = lonlat[17];
			 String LenWid = Mmsi + "," + lonlat1 + "," + shiplength + "," +
			 shipwidth;
			 Coordinate coord = new Coordinate(lon, lat);
			 Point point = geometryFactory.createPoint(coord);
			// if (mp.contains(point)) {
			// context.write(new Text(lonlat2), new Text(LenWid));
			// }
			for (Coordinate coordinate : coordinates) {
					if (coordinate.distance(coord) < 0.00005) {
						String cd =String.valueOf(coordinate.x)+"-"+String.valueOf(coordinate.y);
						context.write(new Text(cd), new Text(LenWid));
					}
				}
			}
		}
	}

	// Reduce 阶段
	public static class IndexCreateReducer extends Reducer<Text, Text, Text, Text> {
		// private DataWritable v = new DataWritable();
		private MultipleOutputs<Text, Text> outputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			outputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, NumberFormatException {

			Text k = new Text();
			Text v = new Text();
			Text vl = new Text();
			Text nul = new Text();
			String filename = key.toString();
			ArrayList<String[]> lineList = new ArrayList<String[]>();
			for (Text value : values) {
				vl = value;
				k.set(key);
				v.set(vl);
				String[] currCol = new String[5];// 包含5个属性值，分别是,mmsi号，经纬度,船只的长度，船只的宽度。
				String data = value.toString();
				currCol = data.split(",");
				lineList.add(currCol);
			}

			double[][] num1 = new double[lineList.size()][5];
			for (int i = 0; i < lineList.size(); i++) {
				for (int j = 0; j < 5; j++) {
					try {
						// 设置一个变量k，避免超过精确度的限制
						if (lineList.get(i)[j].length() > 16) {
							String ks = lineList.get(i)[j].substring(0, 12); // 这个地方需要测试
							num1[i][j] = Double.parseDouble(ks);
						} else {
							num1[i][j] = Double.parseDouble(lineList.get(i)[j]);// 将字符串转化为double类型，存入二维数组
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
			}

			// 计算这个中心点的船只的最大长度和宽度
			double[][] num = new double[num1.length][7];

			for (int i = 0; i < num1.length - 1; i++) {
				double lenMax = 0.0;
				double widMax = 0.0;
				if (num1[i][3] > lenMax) {
					lenMax = num1[i][3];
				}
				if (num1[i][4] > widMax) {
					widMax = num1[i][4];
				}
				// 把数据保存到 数组 num 中,并增加新的列。分别是泊位的最大长度和最最大宽度。
				for (int j = 0; j < 5; j++) {
					num[i][j] = num1[i][j];
				}
				num[i][5] = lenMax;
				num[i][6] = widMax;
			}

			for (int i = 0; i < num.length - 1; i++) {
				String str = "";
				Text result = new Text();
				str = (long) (num[i][0]) + "," + Double.toString(num[i][1]) + "," + Double.toString(num[i][2]) + ","
						+ Double.toString(num[i][3]) + "," + Double.toString(num[i][4]) + ","
						+ Double.toString(num[i][5]) + "," + Double.toString(num[i][6]);
				result.set(str); // String --> Text
				outputs.write(result, nul, filename); // 多文件输出
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("hdfs://192.168.10.32:9000/hadoop/data/common/zzt/middlePoint"), conf);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Berth-length-width-test6");
		job.setJarByClass(LengthAndWidthCal.class);

		job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024);
		job.setNumReduceTasks(37);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(IndexCreateReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/DSLinked_ByMonth/06/";
		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/Berth-length-width/test6/";

		SequenceFileInputFormat.addInputPath(job, new Path(input1));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
