package cn.idatatech.traffic.metro;

import cn.idatatech.traffic.metro.Utils.MetroDataCollector;

import java.io.IOException;
import java.util.Collection;

public class MetroStationRouteUpdate {
    public static void main(String[] args) throws IOException {
        if(args.length == 2) {
            MetroDataCollector metroDataCollector = new MetroDataCollector(args[0]);
            Collection<String[]> data = metroDataCollector.getStationsFromMetro();
            metroDataCollector.writeAsCsv(args[1], data);
        } else {
            System.err.println("Application require two arguments. 1. chrome driver path. 2. csv output path");
        }
    }
}
