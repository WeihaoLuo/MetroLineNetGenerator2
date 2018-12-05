package cn.idatatech.traffic.metro;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import cn.idatatech.traffic.metro.Utils.LocationUtils;
import cn.idatatech.traffic.metro.Utils.MetroBeanProcessor;
import cn.idatatech.traffic.metro.Utils.MetroDataCollector;
import net.bytebuddy.TypeCache;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import javax.validation.constraints.Max;
import java.util.*;
import java.util.stream.Collectors;

public class MetroLineNetUpdate {
    public static void main(String[] args) {
        if (args.length == 3) {
            // 配置spark
            SparkConf sparkConf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName(MetroLineNetUpdate.class.getName())
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
            sparkSession.sqlContext().setConf("spark.sql.parquet.binaryAsString", "true");
            javaSparkContext.setLogLevel("error");

            //数据源位置
            String rawMetroSchemeDataDir = args[0];
            String rawMetroLocationDataDir = args[1];
            String rawMetroStationWithOrderNumDataDir = args[2];

            //读取数据源
            Dataset<MetroLineBean> rawMetroSchemeDS = readMetroTransferSchemeParquetAsDS(sparkSession, rawMetroSchemeDataDir);
                    //.filter("routeName = '广佛线' and endRouteName = '四号线' and stationName = '菊树' and endStationName = '金洲'");

            Dataset<Row> rawMetroLocationDF = sparkSession.read().parquet(rawMetroLocationDataDir);
            Dataset<MetroLineBean> rawMetroStationWithOrderNumDS =
                    readMetroStationWithOrderNumDataAsDS(sparkSession, javaSparkContext, rawMetroStationWithOrderNumDataDir);

            //将每个要换乘线路的站点全部关联并加入到rawMetroSchemeDS
            Dataset<Row> metroWithSubStationDF = addSubStationsToMetroTransferSchemaDS(rawMetroSchemeDS, rawMetroStationWithOrderNumDS);

            //为每个站点添加经纬度
            Dataset<Row> metroWithLocationDF = addLongLatToMetroTransferSchemaDS(metroWithSubStationDF, rawMetroLocationDF);

            //转换为JavaRDD<MetroLineBean>
            MetroBeanProcessor processor = new MetroBeanProcessor();
            JavaRDD<MetroLineBean> metroJavaRDD = processor.convertDatasetRowToMetroLineBean(sparkSession, metroWithLocationDF).toJavaRDD();

            //计算换乘站点之间的距离
            JavaRDD<MetroLineBean> subStationDistJavaRDD = calculateSubStationsDists(metroJavaRDD);

            //转换结果为dataset
            Dataset<MetroLineBean> result = sparkSession.createDataFrame(subStationDistJavaRDD, MetroLineBean.class).as(Encoders.bean(MetroLineBean.class));
            result.orderBy("uuid", "transNum", "subStationNum").selectExpr(
                    "uuid",
                    "routeName",
                    "stationName",
                    "endRouteName",
                    "endStationName",
                    "distance",
                    "transNum",
                    "transRoute",
                    "transStation",
                    "subStationNum",
                    "subStation",
                    "longitude",
                    "latitude"
            ).show(1000);
        } else {
            System.err.println(
                    "Application require three arguments. " +
                            "1. metro transfer plan data path " +
                            "2. metro location data path " +
                            "3. chrome driver path"
            );
        }

    }

    private static Dataset<MetroLineBean> readMetroTransferSchemeParquetAsDS(SparkSession sparkSession, String dataDir) {
        MetroBeanProcessor beanMaker = new MetroBeanProcessor();
        Dataset<Row> dataDF = sparkSession.read().parquet(dataDir).filter("lines_result is not null");
        Dataset<MetroLineBean> beanDataDS = beanMaker.packRawDataAsMetroLineBeansRDD(sparkSession, dataDF);
        return beanDataDS;
    }

    private static Dataset<MetroLineBean> readMetroStationWithOrderNumDataAsDS(SparkSession sparkSession, JavaSparkContext javaSparkContext, String chromeDriverPath) {
        MetroDataCollector metroDataCollector = new MetroDataCollector(chromeDriverPath);
        List<String[]> data = new ArrayList<>(metroDataCollector.getStationsFromMetro());
        MetroBeanProcessor processor = new MetroBeanProcessor();
        List<MetroLineBean> packagedBeanList = data.stream().map(row -> {
            MetroLineBean metroLineBean = new MetroLineBean();
            metroLineBean.setRouteName(processor.cleanContent(row[0]));
            metroLineBean.setSubStationNum(Integer.parseInt(row[1]));
            metroLineBean.setSubStation(processor.cleanContent(row[2]));
            return metroLineBean;
        }).collect(Collectors.toList());
        Dataset<MetroLineBean> result = sparkSession.createDataFrame(javaSparkContext.parallelize(packagedBeanList), MetroLineBean.class)
                .as(Encoders.bean(MetroLineBean.class));
        return result;
    }

    private static Dataset<Row> addSubStationsToMetroTransferSchemaDS(Dataset<MetroLineBean> rawMetroSchemeDS, Dataset<MetroLineBean> subStationDS) {
        //需要用到的列名
        Column metroSchemeRouteName = rawMetroSchemeDS.col("transRoute");
        Column metroSubStationsRouteName = subStationDS.col("routeName");
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

    private static Dataset<Row> addLongLatToMetroTransferSchemaDS(Dataset<Row> metroSchemeDF, Dataset<Row> rawMetroLocationDF) {
        //需要用到的列名
        Column metroSchemeRouteName = metroSchemeDF.col("transRoute");
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

    private static JavaRDD<MetroLineBean> calculateSubStationsDists(JavaRDD<MetroLineBean> data) {
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
                    List<MetroLineBean> oStations = t._2._1;
                    oStations.sort(Comparator.comparing(bean -> bean.getTransNum()));
                    List<MetroLineBean> dStations = inferDfromO(oStations, metroBeanList);

                    //筛选数据，先留下换乘线路相同的站点
                    List<List<MetroLineBean>> stationsBwtOd = getStationsBwtOD(oStations, dStations, metroBeanList);

                    //最后计算两两车站的距离，然后再算总和
                    Integer distance = 0;
                    MetroLineBean beanWithLocationA = null;
                    MetroLineBean beanWithLocationB = null;
                    for (int i = 0; i < stationsBwtOd.size(); i++) {
                        List<MetroLineBean> sortedBeans = stationsBwtOd.get(i).stream()
                                .sorted(Comparator.comparing(bean -> bean.getSubStationNum())).collect(Collectors.toList());
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

    private static List<List<MetroLineBean>> getStationsBwtOD(List<MetroLineBean> oStations, List<MetroLineBean> dStations, List<MetroLineBean> allStations) {
        List<List<MetroLineBean>> stationsBwtOd = new LinkedList<>();
        for (int i = 0; i < oStations.size() - 1; i++) {
            MetroLineBean transferStationA = oStations.get(i);
            MetroLineBean transferStationB = dStations.get(i);
            List<MetroLineBean> stationsInRoute = allStations.stream()
                    .filter(bean -> bean.getTransNum().equals(transferStationA.getTransNum()))
                    .sorted(Comparator.comparing(bean -> bean.getTransNum()))
                    .collect(Collectors.toList());
            if (transferStationA.getSubStationNum() < transferStationB.getSubStationNum()) {
                stationsBwtOd.add(getStationsBwtTwo(transferStationA, transferStationB, stationsInRoute));
            } else {
                stationsBwtOd.add(getStationsBwtTwo(transferStationB, transferStationA, stationsInRoute));
            }
        }
        return stationsBwtOd;
    }

    private static List<MetroLineBean> getStationsBwtTwo(MetroLineBean stationA, MetroLineBean stationB, List<MetroLineBean> stations) {
        List<MetroLineBean> result = stations.stream().filter(bean -> stationA.getSubStationNum() <= bean.getSubStationNum()
                && stationB.getSubStationNum() >= bean.getSubStationNum()).collect(Collectors.toList());
        return result;
    }

    private static List<MetroLineBean> inferDfromO(List<MetroLineBean> odStations, List<MetroLineBean> allStations) {
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

    private static boolean checkIfIsTransferStation(MetroLineBean bean) {
        if (bean.getSubStation().equals(bean.getTransStation())) {
            return true;
        } else {
            return false;
        }
    }

    private static Integer calDist(MetroLineBean bean1, MetroLineBean bean2) {
        Double long1 = Double.valueOf(bean1.getLongitude());
        Double lat1 = Double.valueOf(bean1.getLatitude());
        Double long2 = Double.valueOf(bean2.getLongitude());
        Double lat2 = Double.valueOf(bean2.getLatitude());
        Double distance = LocationUtils.getDistance(long1, lat1, long2, lat2);
        //System.out.println(bean1.getSubStation() + ": " + long1 + ", " + lat1 + ". " + bean2.getSubStation() + ": " + long1 + ", " + lat1 + ". Distance: " + distance);
        return distance.intValue();
    }
}
