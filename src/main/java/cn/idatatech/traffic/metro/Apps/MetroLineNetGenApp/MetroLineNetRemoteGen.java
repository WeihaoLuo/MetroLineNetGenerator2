package cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import cn.idatatech.traffic.metro.Utils.MetroLineNetHttpGetter;
import org.apache.spark.sql.*;

import java.util.List;

public class MetroLineNetRemoteGen extends MetroLineNetGen {
    public MetroLineNetRemoteGen() {
        super(true);
    }

    @Override
    Dataset<MetroLineBean> readMetroStationWithOrderNumDataAsDS(String input) throws Exception {
        MetroLineNetHttpGetter httpGetter = new MetroLineNetHttpGetter();
        List<MetroLineBean> data = httpGetter.readMetroStationsRemotely(input);
        Dataset<MetroLineBean> result = sparkSession.createDataFrame(javaSparkContext.parallelize(data), MetroLineBean.class)
                .as(Encoders.bean(MetroLineBean.class));
        return result;
    }
}
