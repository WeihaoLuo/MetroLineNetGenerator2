package cn.idatatech.traffic.metro.Utils;
import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;


public class MetroBeanProcessor implements Serializable {
    public Dataset<MetroLineBean> convertDatasetRowToMetroLineBean(SparkSession sparkSession, Dataset<Row> data) {
        Dataset<MetroLineBean> beanDS = sparkSession.createDataFrame(data.toJavaRDD().map(row -> {
            MetroLineBean bean = new MetroLineBean();
            if(row.getAs("uuid") != null) bean.setUuid(row.getAs("uuid"));
            if(row.getAs("routeName")!= null) bean.setRouteName(row.getAs("routeName"));
            if(row.getAs("stationId")!= null) bean.setStationId(row.getAs("stationId"));
            if(row.getAs("stationName")!= null) bean.setStationName(row.getAs("stationName"));
            if(row.getAs("endRouteName")!= null) bean.setEndRouteName(row.getAs("endRouteName"));
            if(row.getAs("endStationId")!= null) bean.setEndStationId(row.getAs("endStationId"));
            if(row.getAs("endStationName")!= null) bean.setEndStationName(row.getAs("endStationName"));
            if(row.getAs("distance")!= null)  bean.setDistance(row.getAs("distance"));
            if(row.getAs("count")!= null)  bean.setCount(row.getAs("count"));
            if(row.getAs("price")!= null)  bean.setPrice(row.getAs("price"));
            if(row.getAs("spendTime")!= null)  bean.setSpendTime(row.getAs("spendTime"));
            if(row.getAs("transNum")!= null)  bean.setTransNum(row.getAs("transNum"));
            if(row.getAs("transRoute")!= null)  bean.setTransRoute(row.getAs("transRoute"));
            if(row.getAs("transStation")!= null)  bean.setTransStation(row.getAs("transStation"));
            if(row.getAs("transCode")!= null)  bean.setTransCode(row.getAs("transCode"));
            if(row.getAs("subStationNum")!= null)  bean.setSubStationNum(row.getAs("subStationNum"));
            if(row.getAs("subStation")!= null)  bean.setSubStation(row.getAs("subStation"));
            if(row.getAs("longitude")!= null)  bean.setLongitude(row.getAs("longitude"));
            if(row.getAs("latitude")!= null)  bean.setLatitude(row.getAs("latitude"));
            return bean;
        }), MetroLineBean.class).as(Encoders.bean(MetroLineBean.class));
        return beanDS;
    }

    public Dataset<MetroLineBean> packRawDataAsMetroLineBeansRDD(SparkSession sparkSession, Dataset<Row> rawMetroDataframe) {
        //转换Dataset到JavaRDD<MetroLineBean>形式。注：MetroLineBean里保存的数据结构跟原数据结构不一样
        JavaRDD<MetroLineBean> metroLineBeansRDD = rawMetroDataframe.toJavaRDD().map(row -> {
            //设置换乘线路id
            Integer id = 0;

            //每行的所有换乘路线将保存在此list中
            List<MetroLineBean> rowList = new LinkedList<>();

            //创建读取json的对象
            JsonParser jsonParser = new JsonParser();
            String json = row.getAs("lines_result");

            if (json != null) {
                JsonArray jsonArray = jsonParser.parse(json).getAsJsonArray();
                String uuidForTransfer = UUID.randomUUID().toString();
                //将数据封装到rowBean里
                for (JsonElement element : jsonArray) {
                    JsonObject jsonObj = element.getAsJsonObject();

                    //封装数据。 在封装数据的过程中，MetroLineBean会对部分值的进行转换。比如将‘虫雷 岗’ 转成虫雷岗
                    MetroLineBean metroLineBean = setGeneralAttributes(row, id, uuidForTransfer);

                    //如果LineName为Null,表示此路线与上一个换乘的路线相同
                    if (jsonObj.get("lineName") != null) {
                        metroLineBean.setTransRoute(cleanContent(jsonObj.get("lineName").getAsString()));
                    } else {
                        metroLineBean.setTransRoute(cleanContent(rowList.get(id - 1).getTransRoute()));
                    }
                    metroLineBean.setTransStation(cleanContent(jsonObj.get("stationName").getAsString()));
                    metroLineBean.setTransCode(jsonObj.get("stationCode").getAsString());
                    metroLineBean.setTransNum(id);

                    //添加结果到list中
                    rowList.add(metroLineBean);

                    //为下一个换乘路线id+1
                    id = id + 1;
                }
            } else {
                //封装数据
                MetroLineBean metroLineBean = setGeneralAttributes(row, 0);

                //添加结果到list中
                rowList.add(metroLineBean);
            }
            return rowList;
        }).flatMap(row -> row.listIterator());

        Dataset<MetroLineBean> dataDS = sparkSession.createDataFrame(metroLineBeansRDD, MetroLineBean.class).as(Encoders.bean(MetroLineBean.class));
        return dataDS;
    }

    private MetroLineBean setGeneralAttributes(Row row, Integer id) {
        String uuid = UUID.randomUUID().toString();
        MetroLineBean bean = setGeneralAttributes(row, id, uuid);
        return bean;
    }

    private MetroLineBean setGeneralAttributes(Row row, Integer id, String uuid) {
        MetroLineBean metroLineBean = new MetroLineBean();
        metroLineBean.setUuid(uuid);
        metroLineBean.setRouteName(cleanContent(row.getAs("line_name")));
        metroLineBean.setStationId(row.getAs("stage_id"));
        metroLineBean.setStationName(cleanContent(row.getAs("stage_name")));
        metroLineBean.setEndRouteName(cleanContent(row.getAs("end_line_name")));
        metroLineBean.setEndStationId(row.getAs("end_stage_id"));
        metroLineBean.setEndStationName(cleanContent(row.getAs("end_stage_name")));
        metroLineBean.setCount(row.getAs("count"));
        metroLineBean.setPrice(row.getAs("price"));
        metroLineBean.setSpendTime(row.getAs("spend_time"));
        metroLineBean.setTransNum(id);
        return metroLineBean;
    }
    private String cleanContent(String content) {
        if(content != null) {
            String beforeStr = content;
            //先去除空格
            content = content.trim();

            //如果路线名称包含()而且不包含2号航站楼和1号航站楼, 去掉()和()内的内容。
            if (content.equals("机场北（2号航站楼）")) {
                content = "机场北";
            } else if (content.contains("机场南（1号航站楼）")) {
                content = "机场南";
            } else if (content.equals("十四号线")) {
                content = "知识城线";
            } else if (content.equals("虫雷 岗")) {
                content = "虫雷岗";
            } else if (content.equals("清㘵")) {
                content = "清土布";
            } else if (content.equals("APM")) {
                content = "APM线";
            } else if (content.equals("三北线")) {
                content = "三号线北延线";
            }
            //如果路线名称包含()而且不包含2号航站楼和1号航站楼, 去除()。
            else if(content.contains("(") && content.contains(")")) {
                String strBeforeQuote = content.substring(0, content.indexOf("("));
                String strInQuote = content.substring(content.indexOf("(")+1, content.indexOf(")"));
                //如果括号内有段的字体，改成线
                if(strInQuote.charAt(strInQuote.length()-1) == '段') {
                    strInQuote = strInQuote.substring(0, strInQuote.length()-1) + "线";
                }
                content = strBeforeQuote + strInQuote;
            }
        }
        return content;
    }
}
