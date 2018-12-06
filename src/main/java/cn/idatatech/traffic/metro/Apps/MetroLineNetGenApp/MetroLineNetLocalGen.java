package cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import cn.idatatech.traffic.metro.Utils.MetroBeanProcessor;
import cn.idatatech.traffic.metro.Utils.MetroDataCrawler;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.stream.Collectors;
public class MetroLineNetLocalGen extends MetroLineNetGen {
    public MetroLineNetLocalGen() {
        super(false);
    }

    @Override
    Dataset<MetroLineBean> readMetroStationWithOrderNumDataAsDS(String input) {
        MetroDataCrawler metroDataCrawler = new MetroDataCrawler(input);
        List<String[]> data = new ArrayList<>(metroDataCrawler.getStationsFromMetro());
        MetroBeanProcessor processor = new MetroBeanProcessor();
        List<MetroLineBean> packagedBeanList = data.stream().map(row -> {
            MetroLineBean metroLineBean = new MetroLineBean();
            metroLineBean.setTransRoute(processor.cleanContent(row[0]));
            metroLineBean.setSubStationNum(Integer.parseInt(row[1]));
            metroLineBean.setSubStation(processor.cleanContent(row[2]));
            return metroLineBean;
        }).collect(Collectors.toList());
        Dataset<MetroLineBean> result = sparkSession.createDataFrame(javaSparkContext.parallelize(packagedBeanList), MetroLineBean.class)
                .as(Encoders.bean(MetroLineBean.class));
        return result;
    }
}
