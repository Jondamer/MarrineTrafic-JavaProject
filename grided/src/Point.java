public class Point {
	private double Lat;
	private double Lng;
	public Point(double lat, double lng) {
		super();
		Lat = lat;
		Lng = lng;
	}
	public double getLat() {
		return Lat;
	}
	public void setLat(double lat) {
		Lat = lat;
	}
	public double getLng() {
		return Lng;
	}
	public void setLng(double lng) {
		Lng = lng;
	}
	
}
