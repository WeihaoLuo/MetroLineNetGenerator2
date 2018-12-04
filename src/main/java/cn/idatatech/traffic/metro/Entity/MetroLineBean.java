package cn.idatatech.traffic.metro.Entity;

import java.io.Serializable;

public class MetroLineBean implements Serializable {
    String uuid;
    String routeName;
    String stationId;
    String stationName;
    String endRouteName;
    String endStationId;
    String endStationName;
    Integer distance;
    Integer count;
    Integer price;
    String spendTime;
    Integer transNum;
    String transRoute;
    String transStation;
    String transCode;
    Integer subStationNum;
    String subStation;
    String longitude;
    String latitude;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getRouteName() {
        return routeName;
    }

    public void setRouteName(String routeName) {
        this.routeName = routeName;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public String getEndRouteName() {
        return endRouteName;
    }

    public void setEndRouteName(String endRoute) {
        this.endRouteName = endRoute;
    }

    public String getEndStationId() {
        return endStationId;
    }

    public void setEndStationId(String endStationId) {
        this.endStationId = endStationId;
    }

    public String getEndStationName() {
        return endStationName;
    }

    public void setEndStationName(String endStationName) {
        this.endStationName = endStationName;
    }

    public Integer getDistance() {
        return distance;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public String getSpendTime() {
        return spendTime;
    }

    public void setSpendTime(String spendTime) {
        this.spendTime = spendTime;
    }

    public Integer getTransNum() {
        return transNum;
    }

    public void setTransNum(Integer transNum) {
        this.transNum = transNum;
    }

    public String getTransRoute() {
        return transRoute;
    }

    public void setTransRoute(String transRoute) {
        this.transRoute = transRoute;
    }

    public String getTransStation() {
        return transStation;
    }

    public void setTransStation(String transStation) {
        this.transStation = transStation;
    }

    public String getTransCode() {
        return transCode;
    }

    public void setTransCode(String transCode) {
        this.transCode = transCode;
    }

    public Integer getSubStationNum() {
        return subStationNum;
    }

    public void setSubStationNum(Integer subStationNum) {
        this.subStationNum = subStationNum;
    }

    public String getSubStation() {
        return subStation;
    }

    public void setSubStation(String subStation) {
        this.subStation = subStation;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "MetroLineBean{" +
                "uuid='" + uuid + '\'' +
                ", routeName='" + routeName + '\'' +
                ", stationId='" + stationId + '\'' +
                ", stationName='" + stationName + '\'' +
                ", endRoute='" + endRouteName + '\'' +
                ", endStationId='" + endStationId + '\'' +
                ", endStationName='" + endStationName + '\'' +
                ", distance=" + distance +
                ", count=" + count +
                ", price=" + price +
                ", spendTime='" + spendTime + '\'' +
                ", transNum=" + transNum +
                ", transRoute='" + transRoute + '\'' +
                ", transStation='" + transStation + '\'' +
                ", transCode='" + transCode + '\'' +
                ", subStationNum=" + subStationNum +
                ", subStation='" + subStation + '\'' +
                ", longitude='" + longitude + '\'' +
                ", latitude='" + latitude + '\'' +
                '}';
    }
}
