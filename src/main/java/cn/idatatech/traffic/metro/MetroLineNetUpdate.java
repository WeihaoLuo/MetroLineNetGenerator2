package cn.idatatech.traffic.metro;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import cn.idatatech.traffic.metro.Utils.LocationUtils;
import cn.idatatech.traffic.metro.Utils.MetroBeanProcessor;
import cn.idatatech.traffic.metro.Utils.MetroDataCollector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import java.util.*;
import java.util.stream.Collectors;

public class MetroLineNetUpdate {
    public static void main(String[] args) {
        if (args.length == 3) {
            // 配置spark
            SparkConf sparkConf = new SparkConf()
                    .setMaster("local[1]")
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
            Dataset<Row> rawMetroLocationDF = sparkSession.read().parquet(rawMetroLocationDataDir);
            Dataset<MetroLineBean> rawMetroStationWithOrderNumDS =
                    readMetroStationWithOrderNumDataAsDS(sparkSession, javaSparkContext, rawMetroStationWithOrderNumDataDir);

            //将每个要换乘线路的站点全部关联并加入到rawMetroSchemeDS
            Dataset<Row> metroWithSubStationDF = addSubStationsToMetroTransferSchemaDS(rawMetroSchemeDS, rawMetroStationWithOrderNumDS);

            //为每个站点添加经纬度
            Dataset<Row> metroWithLocationDF = addLongLatToMetroTransferSchemaDS(metroWithSubStationDF, rawMetroLocationDF)
                    .filter("stationName = '金峰' and endStationName = '新塘'")
                    .orderBy("uuid", "transNum", "subStationNum");
            metroWithLocationDF.show(1000);

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
            ).show();
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
        Dataset<Row> dataDF = sparkSession.read().parquet(dataDir)
                .filter("lines_result is not null");
        Dataset<MetroLineBean> beanDataDS = beanMaker.packRawDataAsMetroLineBeansRDD(sparkSession, dataDF);
        return beanDataDS;
    }

    private static Dataset<MetroLineBean> readMetroStationWithOrderNumDataAsDS(SparkSession sparkSession, JavaSparkContext javaSparkContext, String chromeDriverPath) {
        MetroDataCollector metroDataCollector = new MetroDataCollector(chromeDriverPath);
        List<String[]> data = new ArrayList<>(metroDataCollector.getStationsFromMetro());
        List<MetroLineBean> packagedBeanList = data.stream().map(row -> {
            MetroLineBean metroLineBean = new MetroLineBean();
            metroLineBean.setRouteName(row[0]);
            metroLineBean.setSubStationNum(Integer.parseInt(row[1]));
            metroLineBean.setSubStation(row[2]);
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
                .join(newSubStationDS,metroSchemeRouteName.equalTo(metroSubStationsRouteName), "leftOuter")
                .dropDuplicates("uuid","transRoute", "transStation", "subStation")
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
                .dropDuplicates("uuid","transRoute", "transStation", "subStation", "longitude", "latitude")
                .drop(metroLocationRouteName)
                .drop(metroLocationStationName);
        return metroWithLocationDF;
    }

    private static JavaRDD<MetroLineBean> calculateSubStationsDists(JavaRDD<MetroLineBean> data) {
        JavaRDD<MetroLineBean> resutlRDD = data.mapToPair(bean -> new Tuple2<>(bean.getUuid(), bean))
                .combineByKey(
                        bean -> {
                            List<MetroLineBean> beanList = new LinkedList<>();
                            beanList.add(bean);
                            return beanList;
                        },
                        (beanList, bean) -> {
                            beanList.add(bean);
                            return beanList;
                        },
                        (beanList1, beanList2) -> {
                            beanList1.addAll(beanList2);
                            return beanList1;
                        }
                )
                .map(t -> {
                    List<MetroLineBean> metroBeansList = t._2;
                    List<MetroLineBean[]> odTransferStationEachRoute = getODTransferStationInEachRoute(metroBeansList);

                    //计算乘坐地铁od总距离
                    Integer distance = 0;
                    for (int i = 0; i < odTransferStationEachRoute.size()-1; i++) {
                        MetroLineBean transferStartStation = odTransferStationEachRoute.get(i)[0];
                        MetroLineBean transferEndStation = odTransferStationEachRoute.get(i)[1];
                        System.out.println("1. looking for: " + transferStartStation.toString());
                        System.out.println("2. looking for: " + transferEndStation.toString());
                        distance = distance + calDistBwtTransferStations(metroBeansList, transferStartStation, transferEndStation);
                    }

                    //bean赋值
                    List<MetroLineBean> result = new LinkedList<>();
                    for (MetroLineBean[] beanArray : odTransferStationEachRoute) {
                        beanArray[0].setDistance(distance);
                        result.add(beanArray[0]);
                    }
                    return result;
                }).flatMap(list -> list.listIterator());
        return resutlRDD;
    }

    private static Integer calDistBwtTransferStations(List<MetroLineBean> beanList, MetroLineBean transferStartStation, MetroLineBean transferEndStation) {
        //找出换站od两站之间经过所有的站点，并排序
        List<MetroLineBean> sameRouteStations = beanList.stream()
                .filter(bean -> bean.getTransNum().equals(transferStartStation.getTransNum())
                        && bean.getSubStationNum() >= transferStartStation.getSubStationNum()
                        && bean.getSubStationNum() <= transferEndStation.getSubStationNum())
                .sorted(Comparator.comparing(bean -> bean.getSubStationNum()))
                .collect(Collectors.toList());
        System.out.println("##################################################");
        sameRouteStations.forEach(line -> System.out.println(line.toString()));

        //计算距离
        int distance = 0;

        for (int i = 0; i < sameRouteStations.size() - 1; i++) {
            if (sameRouteStations.get(i).getLongitude() != null && sameRouteStations.get(i + 1).getLongitude() != null) {
                distance = distance + calDist(sameRouteStations.get(i), sameRouteStations.get(i + 1));
            } else {
                distance -= 100;
            }
        }
        return distance;
    }

    private static List<MetroLineBean[]> getODTransferStationInEachRoute(List<MetroLineBean> data) {
        //每个数组代表当前线路的上车下车站点
        List<MetroLineBean[]> odTransferStationEachRoute = new LinkedList<>();

        //去除不是换乘车站的站点，然后对换乘站点进行排序
        List<MetroLineBean> transferStations = data.stream()
                .filter(bean -> bean.getTransStation().equals(bean.getSubStation()))
                .sorted(Comparator.comparing(bean -> bean.getTransNum())).collect(Collectors.toList());

        //以两两换乘站点为一个数据放入list中。
        for (int i = 0; i < transferStations.size() - 1; i++) {
            odTransferStationEachRoute.add(new MetroLineBean[]{transferStations.get(i), transferStations.get(i + 1)});
        }
        return odTransferStationEachRoute;
    }


    private static Integer calDist(MetroLineBean bean1, MetroLineBean bean2) {
        Double long1 = Double.valueOf(bean1.getLongitude());
        Double lat1 = Double.valueOf(bean1.getLatitude());
        Double long2 = Double.valueOf(bean2.getLongitude());
        Double lat2 = Double.valueOf(bean2.getLatitude());
        Double distance = LocationUtils.getDistance(long1, lat1, long2, lat2);
        return distance.intValue();

    }
}
