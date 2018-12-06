package cn.idatatech.traffic.metro;


import cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp.MetroLineNetLocalGen;
import cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp.MetroLineNetGen;
import cn.idatatech.traffic.metro.Apps.MetroLineNetGenApp.MetroLineNetRemoteGen;
import org.apache.commons.cli.*;

import javax.naming.NameNotFoundException;

public class AppStart {
    public static void main(String[] args) {
        Options options = new Options();
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;

        Option appName = new Option("n", "appName", true, "avaiable apps: MetroLineNetGenLocal, MetroLineNetGenRemote");
        appName.setRequired(true);
        options.addOption(appName);

        Option metroLocationDir = new Option("l", "metroLocation", true, "Data directory for metro station with long and lat");
        metroLocationDir.setRequired(true);
        options.addOption(metroLocationDir);

        Option metroTransferPlansDir = new Option("t", "metroTransfer", true, "Data directory for metro transfer routes");
        metroTransferPlansDir.setRequired(true);
        options.addOption(metroTransferPlansDir);

        Option metroWebSourcePath = new Option("w", "metroWebSource", true, "Data directory for metro either url or local dir");
        metroWebSourcePath.setRequired(true);
        options.addOption(metroWebSourcePath);

        try {
            cmd = parser.parse(options, args);
            String appType = cmd.getOptionValue("appName");
            String metroLocation = cmd.getOptionValue("metroLocation");
            String metroTransfer = cmd.getOptionValue("metroTransfer");
            String metroWebSource = cmd.getOptionValue("metroWebSource");
            runApp(appType, metroLocation, metroTransfer, metroWebSource);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runApp(String appType, String metroLocation, String metroTransfer, String webSource) throws Exception{
        MetroLineNetGen metroLineNetGen = null;
        switch (appType) {
            case "MetroLineNetGenLocal":
                metroLineNetGen = new MetroLineNetLocalGen();
                break;
            case "MetroLineNetGenRemote":
                metroLineNetGen = new MetroLineNetRemoteGen();
                break;
            default:
        }

        if(metroLineNetGen != null) {
            metroLineNetGen.processMetroLineNet(metroLocation, metroTransfer, webSource);
        } else {
            throw new NameNotFoundException("Can't resolve app name");
        }
    }
}
