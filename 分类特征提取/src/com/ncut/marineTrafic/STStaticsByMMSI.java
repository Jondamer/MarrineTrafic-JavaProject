/**
 *根据速度阈值和停留时长来提取停留点数据
 *
  算法输入:sequenceFile 文件，并按月保存
  P:根据速度阈值和停留时长来提取停留点数据
  算法输出:mmsi文件对应的停留点数据
 */
package com.ncut.marineTrafic;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class STStaticsByMMSI {
	// 设置速度阈值和停留时长这两个全局变量
	// 速度阈值从 0.6---》0.3 ---->0.1
	static double vMin = 0.6;
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
		// map阶段
		@Override
		public void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
			String[] lonlat = value1.toString().split(",");
			if (lonlat.length > 2) {
				try {
					Double lon = Double.parseDouble(lonlat[2]);
					Double lat = Double.parseDouble(lonlat[3]);
					String lonlat2 = String.valueOf(lon) + "," + String.valueOf(lat);
					String utc = lonlat[0];
					String Mmsi = lonlat[1];
					String utclonlat = utc + "," + Mmsi + "," + lonlat2;
					context.write(new Text(Mmsi.toString()), new Text(utclonlat.toString()));
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
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

				String[] currCol = new String[4];// 包含4个属性值，分别是时间戳 ,mmsi号，经纬度
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
			Point firstPoint = new Point(0, 0);
			Point lastPoint = new Point(0, 0);
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
				double T = t2 - t1;
				double[] currCol = new double[5];
				double V = p.getDis(p1, p2) / T; // 不使用集群上的速度，而是使用自己计算得到的速度。
				// 找到'侯停留点' dis 100 -500 -1000
				// 2018.5.9号修改 距离约束从 500 修改为 300
				// 5.11号修改 距离约束为 300 ---》100
				//
				// if (speedPOI < vMin && T < 30 * 60 && p.getDis(p1, p2) < 100)
				// { // 添加两个相邻轨迹点的距离约束
				// for (int j = 0; j < 5; j++) {
				// currCol[j] = (num1[i][j]);
				// }
				// list1.add(currCol);
				if (V < vMin && T < 30 * 60 && p.getDis(p1, p2) < 300) { // 添加两个相邻轨迹点的距离约束
					for (int j = 0; j < 4; j++) {
						currCol[j] = (num1[i][j]);
					}
					currCol[4] = V; // 把计算得到的速度保存起来，而不保存集群上的速度列
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
					// 计算前后十个点的平均速度和平均距离的阈值是否在阈值范围内,如果小于阈值则把这个点看做是一个停留点
					disAvg = sum / 20;
					Vavg = Vsum / 20;
					if (disAvg < 300 && Vavg < vMin) {
						for (int j = 0; j < 4; j++) {
							currCol[j] = (num1[i][j]);
						}
						currCol[4] = Vavg; // 平均速度的值
						list1.add(currCol);
					}

				} else { // 跳出这段循环，接下来判断停留时长是否满足条件
					int list1Length = list1.size();
					if (list1Length > 20) { // 停留点的数目应该大于20
						double lastTime = list1.get(list1Length - 1)[0];
						double firstTime = list1.get(0)[0];
						if (lastTime - firstTime > ST) { // 如果满足停留时长的阈值条件，则把这段'候选停留轨迹段'list1
							// 计算停留时长 st
							double st = (lastTime - firstTime) / 3600.0;
							// 计算体态比
							double lonmin = 180;
							double lonmax = 0;
							double latmin = 90;
							double latmax = 0;
							for (int x = 0; x < list1.size() - 1; x++) {
								double lon = list1.get(x)[2];
								double lat = list1.get(x)[3];
								if (lon < lonmin) {
									lonmin = lon;
								}
								if (lon > lonmax) {
									lonmax = lon;
								}

								if (lat < latmin) {
									latmin = lat;
								}
								if (lat > latmax) {
									latmax = lat;
								}
							}

							Px.setX(lonmin);
							Px.setY(latmin);
							Py.setX(lonmax);
							Py.setY(latmin);
							Pz.setX(lonmin);
							Pz.setY(latmax);
							double temp2 = 0;
							double length = p.getDis(Px, Py);
							double width = p.getDis(Px, Pz);
							if (length < width) {
								temp2 = length;
								length = width;
								width = temp2;
							}
							int pointNum = list1.size();
							double TT= 0;
							double Density = 0;
							//考虑轨迹点在一条线上的情况
							if(width==0 || length==0){
								TT = length;
								Density = pointNum;
							}else{
							// 体态比的值
							TT = length / width;
							// 计算密度							
							Density = pointNum /(length*width);
							}
							// 计算距离方差S
							// 步骤一:先计算平均距离
							double sum = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								// 找到对应的经纬度坐标，并计算其中的值
								PA.setX(list1.get(m)[2]); // 经度
								PA.setY(list1.get(m)[3]); // 纬度
								PB.setX(list1.get(m + 1)[2]);
								PB.setY(list1.get(m + 1)[3]);
								sum = sum + p.getDis(PA, PB);
							}
							double avg = sum / (list1.size());
							// 步骤二：求这段停留点的方差
							double distSum = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								// 找到对应的经纬度坐标，并计算其中的值
								PA.setX(list1.get(m)[2]); // 精度
								PA.setY(list1.get(m)[3]);
								PB.setX(list1.get(m + 1)[2]); // 精度
								PB.setY(list1.get(m + 1)[3]);
								// 先计算平均距离
								double distdemo = p.getDis(PA, PB);
								double distS = distdemo - avg;
								double distPow = Math.pow(distS, 2);
								distSum = distSum + distPow;
							}

							// 方差的值
							double S = distSum / (list1.size());

							// 计算速度的均值和方差

							// 步骤一:先计算累加的速度值
							double velocitySum = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								velocitySum = velocitySum + list1.get(m)[4];
							}
							// 步骤二：求取速度的均值velocityAvg
							double velocityAvg = velocitySum / (list1.size());
							// 步骤三：求取速度的方差 ss
							double ss = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								double V1 = list1.get(m)[4];
								double VS = V1 - velocityAvg;
								double VSPow = Math.pow(VS, 2);
								ss = ss + VSPow;
							}
							/**
							 * 
							 * 计算加速度的均值和方差
							 * 
							 */
							double a_sum = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								double T1 = list1.get(m)[0];
								double T2 = list1.get(m + 1)[0];
								double V1 = list1.get(m)[4];
								double V2 = list1.get(m + 1)[4];
								double a = (V2 - V1) / (T2 - T1); // 加速度
								a_sum = a_sum + a; // 加速度的累加和
							}
							double a_avg = a_sum / (list1.size() - 1); // 加速度的均值
							double a_S = 0; // 加速度的方差声明
							double sum2 = 0;
							for (int m = 0; m < list1.size() - 1; m++) {
								double T1 = list1.get(m)[0];
								double T2 = list1.get(m + 1)[0];
								double V1 = list1.get(m)[4];
								double V2 = list1.get(m + 1)[4];
								double a = (V2 - V1) / (T2 - T1); // 加速度
								double y = a - a_avg;
								sum2 = sum2 + Math.pow(y, 2);
							}
							a_S = sum2 / (list1.size() - 1); // 方差的值

							// /**
							// * 计算最后一个点到最后一点的距离的值，来反映锚地，泊位分布情况的差异
							// *
							// * 两个变量 firstPoint ,lastPoint 两点之间的距离 fir_lastDis
							// */
							firstPoint.setX(list1.get(0)[2]);
							firstPoint.setY(list1.get(0)[3]);
							lastPoint.setX(list1.get(list1.size() - 1)[2]);
							lastPoint.setY(list1.get(list1.size() - 1)[3]);
							double fir_lastDis = Math.abs(p.getDis(lastPoint, firstPoint));
							double maxDis = 0;
							for (int x = 0; x < list1.size() - 1; x++) {
								for (int j = x; j < list1.size() - 1; j++) {
									PA.setX(list1.get(x)[2]); // 经度
									PA.setY(list1.get(x)[3]); // 纬度
									PB.setX(list1.get(j + 1)[2]);
									PB.setY(list1.get(j + 1)[3]);
									double dis = p.getDis(PA, PB);
									if (dis > maxDis) {
										maxDis = dis;
									}
								}
							}

							// 对list1 进行扩充
							for (int m = 0; m < list1.size(); m++) {
								double str1[] = list1.get(m);
								double str2[] = { st };// 停留时长
								double str3[] = { S };// 距离方差的值
								double str4[] = { velocityAvg };// 速度的均值
								double str5[] = { ss };// 速度的方差
								double str6[] = { avg }; // 平均距离
								double str7[] = { a_avg }; // 加速度的均值(有正负值)
								double str8[] = { a_S }; // 加速度的方差
								double str9[] = { fir_lastDis }; // 首末点的距离
								double str10[] = { maxDis };// 停留段最远点集对的距离
								double str11[] = { TT }; // 体态比
								double str12[] = { Density }; // 密度
//								double str13[] = { 0 }; // 标志位

								// 调用数组合并的方法
								double[] d;
//								d = combineArrays2(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11,
//										str12, str13);
//								不带标记位
								d = combineArrays2(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11,
										str12);															
								list2.add(d);
							}

						}
					}
					// 重置'侯选停留点'集合list1
					list1.clear();
					continue;
				}
			}
			// num[]数组保存的每条记录的属性。现在有 5+11个属性列(含标记列)
			double[][] num = new double[list2.size()][16];
			for (int i = 0; i < list2.size(); i++) {
				for (int j = 0; j < 16; j++) {
					num[i][j] = list2.get(i)[j];
				}

			}

			for (int i = 0; i < num.length - 1; i++) {
				String str = "";
				Text result = new Text();
				str = (long) (num[i][0]) + "," + (long) (num[i][1]) + "," + Double.toString(num[i][2]) + ","
						+ Double.toString(num[i][3]) + "," + Double.toString(num[i][4]) + ","
						+ Double.toString(num[i][5]) + "," + Double.toString(num[i][6]) + ","
						+ Double.toString(num[i][7]) + "," + Double.toString(num[i][8]) + ","
						+ Double.toString(num[i][9]) + "," + Double.toString(num[i][10]) + ","
						+ Double.toString(num[i][11]) + "," + Double.toString(num[i][12]) + ","
						+ Double.toString(num[i][13]) + "," + Double.toString(num[i][14]) + ","
						+ Double.toString(num[i][15]);
//						+","+ (long) (num[i][16]);
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
		long milliSeconds = 1000 * 60 * 60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		// Job job = new Job(conf, "BerthTraningData14Features");
		Job job = new Job(conf, "6MonthBoHai11FeaturesV0.6test2");

		job.setJarByClass(STStaticsByMMSI.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(IndexCreateReducer.class);
		//
		job.setNumReduceTasks(37);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setInputFormatClass(KeyValueTextInputFormat.class);

		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
	
		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BoHaiData/6MonthBoHai_IMO/";

//		 String input1 =
//		 "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMO/4MonthIMO-Anchor/";

//		 String input1 =
//		 "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMO/4MonthIMO-Berth/";

//		 String input1 ="hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BerthData/6MonthBerthTrainingData/";
	
//		 String output =
//		 "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMO/4MonthIMO-Berth-11test/";

//		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/AnchorData/5-23/AnchorDemo1-11Features/";
		
//		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BerthData/5-28/6MonthBerthTrainingData-11Features/";
		 
		 String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BoHaiData/6MonthBoHai11FeaturesV0.6test2/ ";
		
//		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/IMO/4MonthIMO-Anchor/";
//		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/BoHaiData/BoHai_IMO-5-28-11Featurestest5/";
		
		
		FileInputFormat.addInputPath(job, new Path(input1));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}