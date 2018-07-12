
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
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class grided {

	public static class GridedMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lonlat = value.toString().split(",");
			if (lonlat.length > 1) { // �����ж�������������ݸ�ʽ���Զ���ִ���
//				double lon = Double.parseDouble(lonlat[6]);
//				double lat = Double.parseDouble(lonlat[7]);
				double lon = Double.parseDouble(lonlat[2]);
				double lat = Double.parseDouble(lonlat[3]);
//				System.out.println("���������" + lon);				
				// ��15�е�����е��� 2λ���е���1λ���е���6λ��
//				if (lonlat.length > 16 && lonlat[15].length() == 2) {

//					int shipType = Integer.parseInt(lonlat[15]);
//					if (shipType >= 80 && shipType <= 89) {
//						 if (shipType >= 60 && shipType <= 69){
						context.write(geohash(lon, lat), one);
//					}
//				}
			}
		}

		public Text geohash(double lon, double lat) {
			int bits = 18;
//			int bits = 20;
			Text text = new Text();
			double lngLeft = -180;
			double lngRight = 180;
			double latTop = 90;
			double latBottom = -90;
			StringBuilder sd = new StringBuilder();
			int i = 0;
			while (i < bits) {
				double lngMid = (lngLeft + lngRight) / 2;
				double latMid = (latTop + latBottom) / 2;

				if (lat > latMid) {
					sd.append("1");
					latBottom = latMid;
				} else {
					sd.append("0");
					latTop = latMid;
				}
				if (lon > lngMid) {
					sd.append("1");
					lngLeft = lngMid;
				} else {
					sd.append("0");
					lngRight = lngMid;
				}
				i++;
			}
			text.set(sd.toString());
			return text;
		}
	}

	public static class GridedReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			int maxNum = 0;
			for (IntWritable val : values) {
				sum += val.get();

			}
			key.set(getCentralPoint(key.toString()) + "," + sum);
			if (sum > maxNum) {
				context.write(key, null);
			}

		}

		/**
		 * ��ȡդ�����ĵ�,�������ĵ㾭γ���ַ�
		 * 
		 * @param code
		 *            geohash����
		 * @return ���ĵ㾭γ���ַ�
		 */
		public String getCentralPoint(String code) {
			int len = code.length();
			int times = 1;
			double latDist = 0; 
			double lngDist = 0; 
			Point pA = new Point(-180, 90); 
			Point pD = new Point(180, -90); 
			int i = 0;
			while (i < len) {
				String currKey = code.substring(i, i + 2);
				latDist = Arith.div(180, Math.pow(2, times));
				lngDist = Arith.div(360, Math.pow(2, times));

				switch (currKey) {
				case "00":
					pA.setLat(pA.getLat() - latDist);
					pD.setLng(pD.getLng() - lngDist);
					break;

				case "01":
					pA.setLng(pA.getLng() + lngDist);
					pA.setLat(pA.getLat() - latDist);
					break;

				case "11":
					pA.setLng(pA.getLng() + lngDist);
					pD.setLat(pD.getLat() + latDist);
					break;

				case "10":
					pD.setLng(pD.getLng() - lngDist);
					pD.setLat(pD.getLat() + latDist);
					break;

				default:
					System.out.println("Error in getting centralpoint , no such key!");
				}

				times++;
				i += 2;
			}

			// �˴����ֽ�latDist��lngDist������Ϊ�������ĵ㾭γ��ֵ��ʡȥ�½������Ĳ���
			// latDist = pA.getLat()-((pA.getLat() - pD.getLat())/2);
			latDist = Arith.div(Arith.add(pA.getLat(), pD.getLat()), 2);// (pA.getLat()+pD.getLat())/2;
			lngDist = Arith.div(Arith.add(pD.getLng(), pA.getLng()), 2);// (pD.getLng()+pA.getLng())/2;

			return lngDist + "," + latDist;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://10.61.2.111:10020");
		// conf.set("mapred.job.tracker", "10.61.2.111:8020");
		// System.setProperty("Hadoop.home.dir", "...");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(grided.class);
		job.setJobName("bohaiwanxibo7-8-9monthPOI18bits");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
//		�Ż�����
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 256*1024*1024);
//        job.setNumReduceTasks(23);  栅格化不需要
        
		job.setMapperClass(GridedMap.class);
		job.setReducerClass(GridedReduce.class);

		// �޸�������
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);  
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

//		job.setOutputFormatClass(TextOutputFormat.class);
		String input1 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanxibo/bohaiwanxibo7-8-9monthPOI/";	
//		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanmaobo/bohaiwanmaobo7-8-9monthPOI/";	
//		String input3 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanmaobo/bohaiwanmaobo-456monthPOI/";	

		
//		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/5MonthIMOPOIEXTpart2/";	
//		String input3 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/6MonthIMOPOIEXT/";	
//		String input4 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/7MonthIMOPOIEXT/";	
//		String input5 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/8MonthIMOPOIEXT/";	
//		String input6 = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/10MonthIMOPOIEXT/";	


//		String input2 = "hdfs://192.168.10.32:9000/hadoop/data/common/ChinaInserted/TankerShipByMMSI07OutOut/";	
//		String input3 = "hdfs://192.168.10.32:9000/hadoop/data/common/ChinaInserted/TankerShipByMMSI08OutOut/";	

		String output = "hdfs://192.168.10.32:9000/hadoop/data/common/zzt/bohaiwanxibo/bohaiwanxibo7-8-9monthPOI18bits/";
						
//    	SequenceFileInputFormat.addInputPath(job, new Path(input1));
    	FileInputFormat.addInputPath(job, new Path(input1));
//    	FileInputFormat.addInputPath(job, new Path(input2)); 
//    	FileInputFormat.addInputPath(job, new Path(input3));
//    	FileInputFormat.addInputPath(job, new Path(input4)); 
//    	FileInputFormat.addInputPath(job, new Path(input5)); 
//    	FileInputFormat.addInputPath(job, new Path(input6)); 



    	FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
}