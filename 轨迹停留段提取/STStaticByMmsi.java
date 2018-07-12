package com.ncut.marrinetrafic;

import java.io.BufferedReader;
import java.io.FileReader;
/**
 * 测试新的运动特征
 */
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class STStaticByMmsi {
	// 设置速度阈值和停留时长这两个全局变量
	// 速度阈值从 0.6---》0.3
	static double vMin = 0.3;
	static double ST = 2 * 3600; // 0.5H ---->2H

	// 补充上一个数组合并的方法
	/**
	 * 利用System.arraycopy()方法
	 * 
	 * @param info
	 * @return
	 */
	public static double[] combineArrays2(double[]... info) {
		int length = 0;
		for (double[] arr : info) {
			length += arr.length;
		}
		double[] result = new double[length];
		int index = 0;
		for (double[] str : info) {
			System.arraycopy(str, 0, result, index, str.length);
			index += str.length;
		}
		return result;
	}

	public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
//		private static HashSet<String> keyWord;
//		private Path[] localFiles;
//		@SuppressWarnings("deprecation")
//		public void setup(Context context) throws IOException, InterruptedException {
//			keyWord = new HashSet<String>();
//			localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//			for (int i = 0; i < localFiles.length; i++) {
//				String a;
//				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
//				while ((a = br.readLine()) != null) {
//					keyWord.add(a);
//				}
//				br.close();
//			}
//
//		}
		// map阶段
		@Override
		public void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
			String[] lonlat = value1.toString().split(",");
			if (lonlat.length > 2) {
					Double lon = Double.parseDouble(lonlat[3]);
					Double lat = Double.parseDouble(lonlat[4]);
					String lonlat2 = String.valueOf(lon) + "," + String.valueOf(lat);
					String utc = lonlat[0];
					String Mmsi = lonlat[1];
					String status = lonlat[2];
					String true_head =  lonlat[6];
					String length = lonlat[7];
					String width = lonlat[8];
					String utclonlat = utc + "," + Mmsi + "," + lonlat2 + "," + status+","+true_head+","+length+","+width;

					
//					if (keyWord.contains(Mmsi)) {
					context.write(new Text(Mmsi.toString()), new Text(utclonlat.toString()));
//				}
			}
		}
	}
	
	// reduce阶段
	static ArrayList<String[]> lineList = new ArrayList<String[]>();

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

			// System.out.println(1);
			ArrayList<String[]> lineList = new ArrayList<String[]>();//
			for (Text value : values) {
				vl = value;
				k.set(key);
				v.set(vl);

				String[] currCol = new String[8];// 包含5个属性值											
				String data = value.toString();
				currCol = data.split(",");
				lineList.add(currCol);
			}

			// 新建一个二维数组，保存动态数组,保存所有轨迹点
			double[][] num1 = new double[lineList.size()][8];
			for (int i = 0; i < lineList.size(); i++) {
				for (int j = 0; j < 8; j++) {
					try {
						// 设置一个变量k，避免超过精确度的限制
						if (lineList.get(i)[j].length() > 16) {
							String ks = lineList.get(i)[j].substring(0, 12);
							num1[i][j] = Double.parseDouble(ks);
						} else {
							num1[i][j] = Double.parseDouble(lineList.get(i)[j]);// 将字符串转化为double类型，存入二维数组
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
			}

			// 按时间戳排序(冒泡排序)
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

			Point p = new Point(0, 0);
			Point p1 = new Point(0, 0);
			Point p2 = new Point(0, 0);
			Point PA = new Point(0, 0);
			Point PB = new Point(0, 0);
			Point Px = new Point(0, 0);
			Point Py = new Point(0, 0);
			Point Pz = new Point(0, 0);
			// Point firstPoint = new Point(0, 0);
			// Point lastPoint = new Point(0, 0);
			// '候选停留点集合'list1 和停留点集合'list2'
			ArrayList<double[]> list1 = new ArrayList<double[]>();
			ArrayList<double[]> list2 = new ArrayList<double[]>();
			for (int i = 0; i < num1.length - 1; i++) {
				p1.setX(num1[i][3]);
				p1.setY(num1[i][2]);
				p2.setX(num1[i + 1][3]);
				p2.setY(num1[i + 1][2]);
				double t1 = num1[i][0];
				double t2 = num1[i + 1][0];
//				int status = (int) (num1[i][4]);
				double T = t2 - t1;
				double[] currCol = new double[9];
				double V = p.getDis(p1, p2) / T; // 不使用集群上的速度，而是使用自己计算得到的速度。
				// 找到'侯停留点' dis 100 -500 -1000
				// 2018.5.9号修改 距离约束从 500 修改为 300
				// 5.11号修改 距离约束为 300 ---》100
				// 速度过大直接过滤轨迹点

				if (V < vMin && T < 30 * 60 && p.getDis(p1, p2) < 300) { // 添加两个相邻轨迹点的距离约束
					for (int j = 0; j < 8; j++) {
						currCol[j] = (num1[i][j]);
					}
					currCol[8] = V; // 把计算得到的速度保存起来，而不保存集群上的速度列

					/**
					 * 提取停留点程序改进
					 * 
					 */
					list1.add(currCol);

				} else if (V > vMin && T < 30 * 60 && p.getDis(p1, p2) > 300 && list1.size() > 10
						&& i < num1.length - 10) {
					Point Pu = new Point(0.0, 0.0);
					Point Pu_1 = new Point(0.0, 0.0);
					double Vavg = 0;
					double disAvg = 0;
					double sum = 0;
					double Vsum = 0;
					for (int u = i - 10; u < i + 10; u++) {
						Pu.setX(num1[u][3]);
						Pu.setY(num1[u][2]);
						Pu_1.setX(num1[u + 1][3]);
						Pu_1.setY(num1[u + 1][2]);
						// 计算前后十个点的平均速度和平均距离的阈值是否在阈值范围内
						sum = sum + p.getDis(Pu, Pu_1);
						Vsum = Vsum + (p.getDis(Pu, Pu_1) / (num1[u + 1][0] - num1[u][0]));
					}
					// 计算前后十个点的平均速度和平均距离的阈值是否在阈值范围内,如果在，则把这个点看做是一个停留点
					disAvg = sum / 20;
					Vavg = Vsum / 20;
					if (disAvg < 300 && Vavg < vMin) {
						for (int j = 0; j < 8; j++) {
							currCol[j] = (num1[i][j]);
						}
						currCol[8] = Vavg; // 平均速度的值
						list1.add(currCol);
					}
				} else { // 跳出这段循环，接下来判断停留时长是否满足条件
					int list1Length = list1.size();
					if (list1Length > 20) {
						double lastTime = list1.get(list1Length - 1)[0];
						double firstTime = list1.get(0)[0];
						if (lastTime - firstTime > ST) { // 如果满足停留时长的阈值条件，则把这段'候选停留轨迹段'list1
							// 计算停留时长st
							double st = (lastTime - firstTime) / 3600.0;
							// 对list1 进行扩充
							for (int m = 0; m < list1.size(); m++) {
								double str1[] = list1.get(m);
								double str2[] = { st };// 停留时长
								double[] d;
								d = combineArrays2(str1, str2);
								list2.add(d);
							}
						}
					}
					// 重置'侯选停留点'集合list1
					list1.clear();
					continue;
				}

			}
			// num[]数组保存的每条记录的属性。
			double[][] num = new double[list2.size()][9];
			for (int i = 0; i < list2.size(); i++) {
				for (int j = 0; j <9; j++) {
					num[i][j] = list2.get(i)[j];
				}

			}

			for (int i = 0; i < num.length - 1; i++) {
				String str = "";
				Text result = new Text();
				str = (long) (num[i][0]) + "," + (long) (num[i][1]) + "," + Double.toString(num[i][2]) + ","
						+ Double.toString(num[i][3]) + "," + +(long) (num[i][4]) + "," + Double.toString(num[i][5])
				        + "," + Double.toString(num[i][6]) + "," + Double.toString(num[i][7]) + "," + Double.toString(num[i][8]);
				result.set(str); // String --> Text
				outputs.write(result, nul, filename); // 多文件输出

			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			outputs.close();
		}
	}

	public static void main(String[] args) throws Exception {
		//
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("hdfs://192.168.10.32:9000/hadoop/data/common/zzt/MmsiExt.csv"), conf);
		long milliSeconds = 1000 * 60 * 60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		// Job job = new Job(conf, "BerthTraningData14Features");
		Job job = new Job(conf, "AMSTERDAM-7-8-9monthxiboPOI");

		job.setJarByClass(STStaticByMmsi.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(IndexCreateReducer.class);
		//
		job.setNumReduceTasks(31);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setInputFormatClass(KeyValueTextInputFormat.class);

		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		 String input1 =
		 "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/AMSTERDAM-7-8-9monthxibo/";
//		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BoHaiData/BoHai_IMO/";
//		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/port1/port1xibo/";
				 
		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/AMSTERDAM-7-8-9monthxiboPOI/";

		FileInputFormat.addInputPath(job, new Path(input1));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
