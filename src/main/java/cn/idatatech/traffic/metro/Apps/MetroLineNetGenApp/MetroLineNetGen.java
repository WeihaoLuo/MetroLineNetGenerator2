package cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import cn.idatatech.traffic.metro.Utils.Common.LocationUtils;
import cn.idatatech.traffic.metro.Utils.MetroBeanProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class MetroLineNetGen implements Serializable {
    protected transient SparkSession sparkSession;
    protected transient JavaSparkContext javaSparkContext;

    public MetroLineNetGen(boolean remote) {
        SparkConf sparkConf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setAppName(MetroLineNetLocalGen.class.getName());
        sparkConf.setMaster("local[*]");
        this.javaSparkContext = new JavaSparkContext(sparkConf);
        this.sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        this.sparkSession.sqlContext().setConf("spark.sql.parquet.binaryAsString", "true");
        javaSparkContext.setLogLevel("error");
    }

    abstract Dataset<MetroLineBean> readMetroStationWithOrderNumDataAsDS(String input) throws Exception;

    public void processMetroLineNet(String rawMetroLocationDataDir, String rawMetroSchemeDataDir, String rawMetroStationWithOrderNumDataDir) throws Exception {

        //读取数据源
        Dataset<MetroLineBean> rawMetroSchemeDS = readMetroTransferSchemeParquetAsDS(rawMetroSchemeDataDir);
        Dataset<Row> rawMetroLocationDF = this.sparkSession.read().parquet(rawMetroLocationDataDir);
        Dataset<MetroLineBean> rawMetroStationWithOrderNumDS =
                readMetroStationWithOrderNumDataAsDS(rawMetroStationWithOrderNumDataDir);

        //将每个要换乘线路的站点全部关联并加入到rawMetroSchemeDS
        Dataset<Row> metroWithSubStationDF = addSubStationsToMetroTransferSchemaDS(rawMetroSchemeDS, rawMetroStationWithOrderNumDS);

        //为每个站点添加经纬度
        Dataset<Row> metroWithLocationDF = addLongLatToMetroTransferSchemaDS(metroWithSubStationDF, rawMetroLocationDF);
        Dataset<Row> repartitionMetroDF = metroWithLocationDF.repartition(metroWithLocationDF.col("transRoute"));

        //转换为JavaRDD<MetroLineBean>
        MetroBeanProcessor processor = new MetroBeanProcessor();
        JavaRDD<MetroLineBean> metroJavaRDD = processor.convertDatasetRowToMetroLineBean(sparkSession, repartitionMetroDF).toJavaRDD();

        //计算换乘站点之间的距离
        JavaRDD<MetroLineBean> subStationDistJavaRDD = calculateSubStationsDists(metroJavaRDD);

        //转换结果为dataset
        Dataset<Row> result = sparkSession
                .createDataFrame(subStationDistJavaRDD, MetroLineBean.class).as(Encoders.bean(MetroLineBean.class))
                .selectExpr(
                        "uuid",
                        "routeName as from_route_name",
                        "stationId as from_station_id",
                        "stationName as from_station_name",
                        "endRouteName as dest_route_name",
                        "stationId as dest_station_id",
                        "endStationName as dest_station_name",
                        "distance",
                        "count",
                        "price",
                        "spendTime as spend_time",
                        "transCode as transfer_code",
                        "transNum as transfer_order",
                        "transRoute as transfer_route_name",
                        "transStation as transfer_station_name",
                        "subStationNum as sub_station_num",
                        "subStation as sub_station_name",
                        "longitude",
                        "latitude"
                );

        result.show();
    }

    protected Dataset<MetroLineBean> readMetroTransferSchemeParquetAsDS(String dataDir) {
        MetroBeanProcessor beanMaker = new MetroBeanProcessor();
        Dataset<Row> dataDF = this.sparkSession.read().parquet(dataDir).filter("lines_result is not null");
        Dataset<MetroLineBean> beanDataDS = beanMaker.packRawDataAsMetroLineBeansRDD(this.sparkSession, dataDF);
        return beanDataDS;
    }

    protected Dataset<Row> addSubStationsToMetroTransferSchemaDS(Dataset<MetroLineBean> rawMetroSchemeDS, Dataset<MetroLineBean> subStationDS) {
        //需要用到的列名
        Column metroSchemeRouteName = rawMetroSchemeDS.col("transRoute");
        Column metroSubStationsRouteName = subStationDS.col("transRoute");
        Column metroSubStationsName = subStationDS.col("subStation");
        Column metroSubStationsNum = subStationDS.col("subStationNum");

        //去掉不需要的列名
        Dataset<Row> metroSchemeDF = rawMetroSchemeDS.drop("subStation", "subStationNum", "subStationNum", "longitude", "latitude");
        Dataset<Row> newSubStationDS = subStationDS.select(metroSubStationsRouteName, metroSubStationsName, metroSubStationsNum);

        //添加换乘需要路过的站点
        Dataset<Row> metroWithSubStationDF = metroSchemeDF
                .join(newSubStationDS, metroSchemeRouteName.equalTo(metroSubStationsRouteName), "leftOuter")
                .dropDuplicates("uuid", "transRoute", "transStation", "subStation")
                .drop(metroSubStationsRouteName);
        return metroWithSubStationDF;
    }

    protected Dataset<Row> addLongLatToMetroTransferSchemaDS(Dataset<Row> metroSchemeDF, Dataset<Row> rawMetroLocationDF) {
        //需要用到的列名
        Column metroSchemeSubStation = metroSchemeDF.col("subStation");
        Column metroLocationRouteName = rawMetroLocationDF.col("route_name");
        Column metroLocationStationName = rawMetroLocationDF.col("station_name");
        Column metroLocationLongitude = rawMetroLocationDF.col("longitude");
        Column metroLocationLatitude = rawMetroLocationDF.col("latitude");

        //去掉不需要的列名
        Dataset<Row> newMetroLocationDF = rawMetroLocationDF.select(metroLocationRouteName, metroLocationStationName, metroLocationLongitude, metroLocationLatitude);

        //添加经纬度
        Dataset<Row> metroWithLocationDF = metroSchemeDF.join(
                newMetroLocationDF,
                metroSchemeSubStation.equalTo(metroLocationStationName), "leftOuter")
                .dropDuplicates("uuid", "transRoute", "transStation", "subStation", "longitude", "latitude")
                .drop(metroLocationRouteName)
                .drop(metroLocationStationName);
        return metroWithLocationDF;
    }

    protected JavaRDD<MetroLineBean> calculateSubStationsDists(JavaRDD<MetroLineBean> data) {
        JavaRDD<MetroLineBean> resutlRDD = data.mapToPair(bean -> new Tuple2<>(bean.getUuid(), bean))
                .combineByKey(
                        bean -> {
                            List<MetroLineBean> allStationsInaTrip = new LinkedList<>();
                            List<MetroLineBean> transferStations = new LinkedList<>();
                            allStationsInaTrip.add(bean);
                            if (checkIfIsTransferStation(bean)) transferStations.add(bean);
                            return new Tuple2<>(transferStations, allStationsInaTrip);
                        },
                        (stationsTuple, bean) -> {
                            stationsTuple._2.add(bean);
                            if (checkIfIsTransferStation(bean)) stationsTuple._1.add(bean);
                            return stationsTuple;
                        },
                        (stationsTuple1, stationsTuple2) -> {
                            stationsTuple1._1.addAll(stationsTuple2._1);
                            stationsTuple1._2.addAll(stationsTuple2._2);
                            return stationsTuple1;
                        }
                )
                .map(t -> {
                    List<MetroLineBean> metroBeanList = t._2._2;

                    //通过入站o点，推导路线内的另一个换乘站点(d点)，保存每个o点推导出来d点的站序号到List中
                    List<MetroLineBean> oStations = t._2._1.stream()
                            .sorted(Comparator.comparing(MetroLineBean :: getTransNum)).collect(Collectors.toList());
                    List<MetroLineBean> dStations = inferDfromO(oStations, metroBeanList);

                    //筛选数据，先留下换乘线路相同的站点
                    List<List<MetroLineBean>> stationsBwtOd = getStationsBwtOD(oStations, dStations, metroBeanList);

                    //最后计算两两车站的距离，然后再算总和
                    Integer distance = 0;
                    MetroLineBean beanWithLocationA = null;
                    MetroLineBean beanWithLocationB = null;
                    for (int i = 0; i < stationsBwtOd.size(); i++) {
                        List<MetroLineBean> sortedBeans = stationsBwtOd.get(i).stream()
                                .sorted(Comparator.comparing(MetroLineBean :: getSubStationNum)).collect(Collectors.toList());
                        for (int x = 0; x < stationsBwtOd.get(i).size() - 1; x++) {
                            MetroLineBean stationA = sortedBeans.get(x);
                            MetroLineBean stationB = sortedBeans.get(x + 1);

                            if(stationA.getLongitude() == null) {
                                stationA = beanWithLocationA;
                            }

                            if(stationB.getLongitude() == null){
                                stationB = beanWithLocationB;
                            }

                            if (stationA != null && stationB != null && stationA.getLongitude() != null && stationB.getLongitude() != null) {
                                beanWithLocationA = stationA;
                                beanWithLocationB = stationB;
                                distance = distance + calDist(stationA, stationB);
                            }
                        }
                    }

                    List<MetroLineBean> result = stationsBwtOd.stream()
                            .flatMap(list -> list.stream())
                            .collect(Collectors.toList());

                    for (MetroLineBean bean : result) {
                        bean.setDistance(distance);
                    }

                    return result;
                }).flatMap(list -> list.listIterator());
        return resutlRDD;
    }

    protected List<List<MetroLineBean>> getStationsBwtOD(List<MetroLineBean> oStations, List<MetroLineBean> dStations, List<MetroLineBean> allStations) {
        List<List<MetroLineBean>> stationsBwtOd = new LinkedList<>();
        for (int i = 0; i < oStations.size() - 1; i++) {
            MetroLineBean transferStationA = oStations.get(i);
            MetroLineBean transferStationB = dStations.get(i);
            List<MetroLineBean> stationsInRoute = allStations.stream()
                    .filter(bean -> bean.getTransNum().equals(transferStationA.getTransNum()))
                    .sorted(Comparator.comparing(MetroLineBean :: getTransNum))
                    .collect(Collectors.toList());
            if (transferStationA.getSubStationNum() < transferStationB.getSubStationNum()) {
                stationsBwtOd.add(getStationsBwtTwo(transferStationA, transferStationB, stationsInRoute));
            } else {
                stationsBwtOd.add(getStationsBwtTwo(transferStationB, transferStationA, stationsInRoute));
            }
        }
        return stationsBwtOd;
    }

    protected List<MetroLineBean> getStationsBwtTwo(MetroLineBean stationA, MetroLineBean stationB, List<MetroLineBean> stations) {
        return stations.stream().filter(bean -> stationA.getSubStationNum() <= bean.getSubStationNum()
                && stationB.getSubStationNum() >= bean.getSubStationNum()).collect(Collectors.toList());
    }

    protected List<MetroLineBean> inferDfromO(List<MetroLineBean> odStations, List<MetroLineBean> allStations) {
        List<MetroLineBean> dStationsOrderNumList = new LinkedList<>();
        for (int i = 0; i < odStations.size() - 1; i++) {
            MetroLineBean stationBeforeTransfer = odStations.get(i);
            MetroLineBean stationAfterTransfer = odStations.get(i + 1);
            MetroLineBean dStation = allStations.stream()
                    .filter(bean -> bean.getTransRoute().equals(stationBeforeTransfer.getTransRoute())
                            && bean.getSubStation().equals(stationAfterTransfer.getSubStation())).findAny().get();
            dStationsOrderNumList.add(dStation);
        }
        return dStationsOrderNumList;
    }

    protected boolean checkIfIsTransferStation(MetroLineBean bean) {
        return bean.getSubStation().equals(bean.getTransStation());
    }

    protected Integer calDist(MetroLineBean bean1, MetroLineBean bean2) {
        Double long1 = Double.valueOf(bean1.getLongitude());
        Double lat1 = Double.valueOf(bean1.getLatitude());
        Double long2 = Double.valueOf(bean2.getLongitude());
        Double lat2 = Double.valueOf(bean2.getLatitude());
        Double distance = LocationUtils.getDistance(long1, lat1, long2, lat2);
        return distance.intValue();
    }
}
