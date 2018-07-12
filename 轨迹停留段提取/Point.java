/*����ÿ���켣������*/
package com.ncut.marrinetrafic;

import org.apache.batik.css.engine.value.StringValue;

public class Point {
	private double x, y;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public Point(double x, double y) {
		super();
		this.x = x;
		this.y = y;

	}

	double getDistance(Point p) {
		return (x - p.x) * (x - p.x) + (y - p.y) * (y - p.y);
	}

	// 
	public double getDis(Point p1, Point p2) {
		double radlat1 = 3.1415926 / 180 * p1.x;
		double radlat2 = 3.1415926 / 180 * p2.x;
		double a = 3.1415926 / 180 * p1.x - 3.1415926 / 180 * p2.x;
		double b = 3.1415926 / 180 * p1.y - 3.1415926 / 180 * p2.y;
		double s = 2 * Math.asin(Math.sqrt(
				Math.pow(Math.sin(a / 2), 2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.pow(Math.sin(b / 2), 2)));
		double earth_radius = 6378.137;
		// 计算得到的两个轨迹点之间的距离的单位是 m
		s = s * earth_radius * 1000;
		if (s < 0)
			return (-s);
		else
			return (s);
	}

	/**
	 * 计算经度方向上的跨度或者纬度上的跨度
	 * 
	 * @param args
	 */

	public static void main(String[] args) {
		Point p = new Point(0, 0);
		Point p1 = new Point(119.22814199999999, 39.134867);
		Point p2 = new Point(119.22814199999999, 39.134863);
		System.out.println("hh:" + p.getDis(p1, p2));
		/**
		 * 测试一下
		 */
		String s = "3.7012791097189985E-";
		String se = "0.37012791097189984566444477";

		// double ses=Double.parseDouble(se);
		//
		// String sss =String.valueOf(ses)+"";
		// System.out.println(sss);

		// double ss=Double.parseDouble(se);
		// System.out.println("ss:"+ss);
		// System.out.println("s:"+s);
		// System.out.println("ss:"+s.length());
	}

}