package cn.idatatech.traffic.metro.Utils.Common;

/**
 * 经纬度距离计算工具类 
 * TODO 逻辑距离，并非真实距离，真实距离调地图接口计算。
 * 
 * @author neworld
 *
 */
public class LocationUtils {

	private static final double EARTH_RADIUS = 6378.137;

	private static double getRadian(double d) {
		return d * Math.PI / 180.0;
	}

	/**
	 * 通过经纬度获取距离(单位：米)
	 * 
	 * @param lat1
	 *            1点的纬度
	 * @param lng1
	 *            1点的经度
	 * @param lat2
	 *            2点的纬度
	 * @param lng2
	 *            2点的经度
	 * @return 距离 单位 米
	 */
	public static double getDistance(double lng1, double lat1,  double lng2, double lat2) {
		double radLat1 = getRadian(lat1);
		double radLat2 = getRadian(lat2);
		double a = radLat1 - radLat2;
		double b = getRadian(lng1) - getRadian(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 10000d) / 10000d;
		s = s * 1000;
		return s;
	}
}
