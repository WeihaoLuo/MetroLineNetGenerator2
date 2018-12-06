package cn.idatatech.traffic.metro.Utils;

import cn.idatatech.traffic.metro.Entity.MetroLineBean;
import com.google.gson.*;

import java.io.*;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class MetroLineNetHttpGetter {
    public List<MetroLineBean> readMetroStationsRemotely(String url) throws Exception {
        //Get json
        JsonParser jp = new JsonParser();
        JsonElement data = jp.parse(readUrl(url)).getAsJsonObject().get("lines");
        JsonArray routes = data.getAsJsonArray();
        MetroBeanProcessor processor = new MetroBeanProcessor();
        List<MetroLineBean> result = new LinkedList<>();
        for (JsonElement route : routes) {
            JsonObject routeObj = route.getAsJsonObject();
            JsonArray stations = routeObj.get("stages").getAsJsonArray();
            String lineName = processor.cleanContent(routeObj.get("lineName").getAsString());
            int orderNum = 0;
            for (JsonElement station : stations) {
                JsonObject stationObj = station.getAsJsonObject();
                result.add(packDataAsBean(processor, stationObj, orderNum, lineName));
                orderNum += 1;
            }
        }
        return result;
    }

    private String readUrl(String urlString) throws Exception {
        BufferedReader reader = null;
        try {
            URL url = new URL(urlString);
            reader = new BufferedReader(new InputStreamReader(url.openStream()));
            StringBuffer buffer = new StringBuffer();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1)
                buffer.append(chars, 0, read);

            String jsonStr = buffer.toString();
            String cleanJsonStr = jsonStr.substring(0, jsonStr.length() - 1).replace("jQuery18003103297813884627_1543974969521(", "");
            return cleanJsonStr;
        } finally {
            if (reader != null)
                reader.close();
        }
    }

    private MetroLineBean packDataAsBean(MetroBeanProcessor processor, JsonObject jsonObj, Integer orderNum, String lineName) {
        MetroLineBean bean = new MetroLineBean();
        bean.setTransRoute(lineName);
        bean.setStationId(processor.cleanContent(jsonObj.get("stageId").getAsString()));
        bean.setSubStation(processor.cleanContent(jsonObj.get("stageName").getAsString()));
        bean.setSubStationNum(orderNum);
        return bean;
    }
}
