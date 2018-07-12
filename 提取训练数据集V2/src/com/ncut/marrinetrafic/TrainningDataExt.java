 package com.ncut.marrinetrafic;

/**
 * 从集群上提取训练数据
 * @author Administrator
 *
 */

/**
 *根据速度阈值和停留时长来提取停留点数据
 *
  算法输入:sequenceFile 文件，并按月保存
  P:根据速度阈值和停留时长来提取停留点数据
  算法输出:mmsi文件对应的停留点数据
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

@SuppressWarnings("deprecation")
public class TrainningDataExt {
//	private static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
	public static class MyMap extends Mapper<Text, Text, Text, Text> {
		private static GeometryFactory geometryFactory = new GeometryFactory();
		WKTReader reader = new WKTReader();
		private static HashSet<String> keyWord;// 保存所有的多边形的数据
		private static ArrayList<Polygon> arrayList = new ArrayList<>(); // 保存所有的多边形
		private Path[] localFiles;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			keyWord = new HashSet<String>();
			localFiles = org.apache.hadoop.mapreduce.filecache.DistributedCache
					.getLocalCacheFiles(context.getConfiguration());
			// 步骤一 ：把多边形字符串保存到集合HashSet中
			for (int i = 0; i < localFiles.length; i++) {
				String a;
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((a = br.readLine()) != null) {
					keyWord.add(a);
				}
				br.close();
			}
			// 步骤二 ：遍历集合，生成多边形，并保存到另外一个多边形的集合中。
			// ArrayList<Polygon> arrayList = new ArrayList<>();
			for (String s : keyWord) {
				System.out.println(s);
					try {
						Polygon py = (Polygon) reader.read(s);
						arrayList.add(py);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}											
			}
		}

		// map方法
		@Override
		public void map(Text key1, Text value1, Context context) throws IOException, InterruptedException {
			String[] lonlat = value1.toString().split(",");
			if (lonlat.length > 25) {
				try {
					Double lon = Double.parseDouble(lonlat[6]);
					Double lat = Double.parseDouble(lonlat[7]);
					String lonlat2 = String.valueOf(lon) + "," + String.valueOf(lat);
					String utc = lonlat[0];
					String Mmsi = lonlat[1];
					double speed = Double.parseDouble(lonlat[25]);
					String utclonlat = utc + ","+ lonlat2 + "," + speed;
					Coordinate coord = new Coordinate(lon, lat);
					Point point = geometryFactory.createPoint(coord);
					for (int i = 0; i < arrayList.size(); i++) {
						Polygon poly = arrayList.get(i);
//						data2 加上了速度约束  speed <1   .data3不加上速度约束
						if (poly.contains(point)) {
							context.write(new Text(Mmsi), new Text(utclonlat));
						}
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
			}

		}
	}

	// reduce阶段
	public static class MyReduce extends Reducer<Text, Text, Text, Text> {
		//private DataWritable v = new DataWritable();
		private MultipleOutputs<Text, Text> outputs; 		
		@Override  
		protected void setup(Context context) throws IOException, InterruptedException {  
	            	outputs = new MultipleOutputs<Text, Text>(context);  
	        }  

		@Override
		public void reduce(Text key2, Iterable<Text> values2, Context context)
				throws IOException, InterruptedException {
			Text k = new Text();
			Text v = new Text();
			Text vl = new Text();
			Text nul = new Text();
			String filename = key2.toString();

			// System.out.println(1);
			ArrayList<String[]> lineList = new ArrayList<String[]>();//
			for (Text value : values2) {
				vl = value;
				k.set(key2);
				v.set(vl);

				String[] currCol = new String[4];// 包含4个属性值，分别是时间戳 经纬度、速度
				String data = value.toString();
				currCol = data.split(",");
				lineList.add(currCol);
			}

			// 新建一个二维数组，保存动态数组,保存所有轨迹点
			double[][] num1 = new double[lineList.size()][4];
			for (int i = 0; i < lineList.size(); i++) {
				for (int j = 0; j < 4; j++) {
					try {
						// 设置一个变量k，避免超过精确度的限制
						if (lineList.get(i)[j].length() > 16) {
							String ks = lineList.get(i)[j].substring(0, 10);
							num1[i][j] = Double.parseDouble(ks);
						} else {
							num1[i][j] = Double.parseDouble(lineList.get(i)[j]);// 将字符串转化为double类型，存入二维数组
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
			}

			// 按时间戳排序
			double[] temp;
			for (int i = 0; i < lineList.size() - 1; i++) {
				for (int j = 0; j < lineList.size() - 1 - i; j++) {
					if (num1[j][0] > num1[j + 1][0]) {
						temp = num1[j];
						num1[j] = num1[j + 1];
						num1[j + 1] = temp;
					}
				}
			}

			ArrayList<String[]> list1 = new ArrayList<String[]>();

			for (int i = 0; i < num1.length - 1; i++) {

//				double speedPOI = num1[i][3];
				double t1 = num1[i][0];
				double t2 = num1[i + 1][0];
				double T = t2 - t1;
				String[] currCol = new String[4];

				if (T != 0) { // 比较相邻的两个时间戳，如果两个时间相等，则保留一个点。
					for (int j = 0; j < 4; j++) {
						currCol[j] = String.valueOf(num1[i][j]); 																	
																		
					}
					list1.add(currCol);
				}
			}
			double[][] num = new double[list1.size()][4];
			for (int i = 0; i < list1.size(); i++) {
				for (int j = 0; j < 4; j++) {
					num[i][j] = Double.parseDouble(list1.get(i)[j]);// 将字符串转化为double类型，存入二维数组
				}
			}

			for (int i = 0; i < num.length - 1; i++) {
				String str = "";
				Text result = new Text();
				str = (long) (num[i][0]) + "," + Double.toString(num[i][1]) + "," + Double.toString(num[i][2]) + ","
						+ Double.toString(num[i][3]);
				result.set(str); // String --> Text
				outputs.write(result, nul, filename); // 多文件输出
			}
		}
			    			    

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		org.apache.hadoop.mapreduce.filecache.DistributedCache
				.addCacheFile(new URI("hdfs://192.168.10.32:9000/hadoop/data/common/zzt/berth_data4_28.txt"), conf);
		
		Job job = new Job(conf, "6MonthBerthTranningDataExtsortandquchong");
		job.setJarByClass(TrainningDataExt.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat.class);
		job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		//
		job.setNumReduceTasks(43);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		// 去除默认的空文件
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/DSLinked_ByMonth/06/";
//		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/Test/POITest/";
		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/6MonthBerthTrainingData4_28";
		FileInputFormat.addInputPath(job, new Path(input1));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
