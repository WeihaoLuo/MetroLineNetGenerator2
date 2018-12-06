package cn.idatatech.traffic.metro.Apps.MetroRouteCrawlApp;

import cn.idatatech.traffic.metro.Utils.MetroDataCrawler;

import java.io.IOException;
import java.util.Collection;

public class MetroRouteCrawl {
    public static void main(String[] args) throws IOException {
        if(args.length == 2) {
            MetroDataCrawler metroDataCrawler = new MetroDataCrawler(args[0]);
            Collection<String[]> data = metroDataCrawler.getStationsFromMetro();
            metroDataCrawler.writeAsCsv(args[1], data);
        } else {
            System.err.println("Application require two arguments. 1. chrome driver path. 2. csv output path");
        }
    }
}
