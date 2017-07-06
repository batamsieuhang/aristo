package org.apache.storm.starter.profiling;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.bolt.AggregatorNumberBolt;
import org.apache.storm.starter.bolt.GeneralNumberbolt;
import org.apache.storm.starter.bolt.MultipleStreamNumberBolt;
import org.apache.storm.starter.metrics.BoltMetrics;
import org.apache.storm.starter.metrics.BoltMetricsUpdater;
import org.apache.storm.starter.metrics.ComponentMetricsCreator;
import org.apache.storm.starter.spout.MultipleStreamSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Created by VagelisAkis on 20/1/2017.
 */
public class TopologySubmitterMG {

    private String topologyName;
    private File file;


    public TopologySubmitterMG(String topologyName){
        this.topologyName = topologyName;
        file = new File(topologyName + ".txt");
    }

    public void writeData(BoltMetrics boltMetrics, double avgAckedRate) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        writer.println(avgAckedRate + " , " + boltMetrics.getAckedRate());
        writer.close();
    }

    public void writeData(int workers, int spout, int general, int aggregate, int general2)throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";
        writer.println(workers + comma + spout + comma + general + comma + aggregate + comma + general2);
        writer.close();
    }

    public void submitTopology(int workers, int spout, int general, int aggregate, int general2) throws Exception{
        writeData(workers, spout, general, aggregate, general2);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MultipleStreamSpout(2), spout);

        builder.setBolt("general", new GeneralNumberbolt(), general).shuffleGrouping("spout", "0");
        builder.setBolt("aggregate", new AggregatorNumberBolt(), aggregate).shuffleGrouping("general").shuffleGrouping("spout","1");
        builder.setBolt("general2", new GeneralNumberbolt(), general2).shuffleGrouping("aggregate");
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 60000);

        conf.setNumWorkers(workers);
        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }

    public void killTopology() throws Exception {

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);

        client.killTopologyWithOpts(topologyName, opts);
    }

    private void monitorTopology() throws Exception{

        ComponentMetricsCreator bolt = new ComponentMetricsCreator(topologyName, "general");

        BoltMetricsUpdater boltMetricsUpdater = (BoltMetricsUpdater)bolt.getComponentUpdater();

        boltMetricsUpdater.updateMetrics();
        Thread.sleep(40*1000);

        BoltMetrics boltMetrics = boltMetricsUpdater.getBoltMetrics();

        BoltMetrics outBoltMetrics = boltMetricsUpdater.getBoltMetrics();

        int retry = 5;
//        double avgAckedRate = outBoltMetrics.getAckedRate();
//        int counter = 1;
        double avgAckedRate = 0;
        int counter = 0;

        while(true) {
            boltMetricsUpdater.updateMetrics();

            boltMetrics = boltMetricsUpdater.getBoltMetrics();

            avgAckedRate += boltMetrics.getAckedRate();
            counter++;

            if(boltMetrics.getAckedRate() > outBoltMetrics.getAckedRate()) {
                if ( (boltMetrics.getAckedRate() - outBoltMetrics.getAckedRate()) / outBoltMetrics.getAckedRate() < 0.05 )
                    retry--;

                outBoltMetrics = boltMetrics;
            }
            else {
                retry--;
            }

            if (retry == 0)
                break;

            Thread.sleep(60*1000);

        }

        writeData(outBoltMetrics, avgAckedRate/counter);

    }

    public static void main(String args[]) {
        try {

            TopologySubmitterMG t = new TopologySubmitterMG("testMG");

            for (int i = 1; i <= 7; i++) { //worker
                for (int j = 1; j <= i; j++) { //spout
                    for (int k = 1; k <= i; k++) { //general
                        for (int w = 1; w <= i; w++) { //aggregate
                            for (int l = 1; l <= w; l++) { //general2
                                System.out.println("Submitting topology : (" + i + "," + j + "," + k + "," + w + ") " + "Waiting for 30 sec");
                                t.submitTopology(i, j, k, w, l);

                                Thread.sleep(30 * 1000);
                                t.monitorTopology();

                                System.out.println("Killing topology : (" + i + "," + k + "," + j + "," + w + ") " + "Waiting for 20 sec");
                                t.killTopology();
                                Thread.sleep(20 * 1000);
                            }
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
