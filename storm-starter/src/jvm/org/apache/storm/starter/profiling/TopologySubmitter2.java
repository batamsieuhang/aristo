package org.apache.storm.starter.profiling;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.bolt.SplitSentence;
import org.apache.storm.starter.bolt.WordCount;
import org.apache.storm.starter.metrics.*;
import org.apache.storm.starter.spout.FastRandomSentenceSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
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
public class TopologySubmitter2 {

    private String topologyName;
    private File file;


    public TopologySubmitter2(String topologyName){
        this.topologyName = topologyName;
        file = new File(topologyName + ".txt");
    }

    public void writeData(SpoutMetrics spoutMetrics, double avgAckedRate) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        writer.println(avgAckedRate + " , " + spoutMetrics.getAckedRate() + " , " + spoutMetrics.getCompleteLatency());
        writer.close();
    }

    public void writeData(int workers, int spout, int split, int count)throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";
        writer.println(workers + comma + spout + comma + split + comma + count);
        writer.close();
    }

    public void submitWCTopology(int workers, int spout, int split, int count) throws Exception{
        writeData(workers, spout, split, count);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FastRandomSentenceSpout(), spout);

        builder.setBolt("split", new SplitSentence(), split).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(),count).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);

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

        ComponentMetricsCreator spout = new ComponentMetricsCreator(topologyName, "spout");

        SpoutMetricsUpdater spoutMetricsUpdater = (SpoutMetricsUpdater)spout.getComponentUpdater();

        spoutMetricsUpdater.updateMetrics();
        Thread.sleep(30*1000);

        SpoutMetrics spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();

        SpoutMetrics outSpoutMetrics = spoutMetricsUpdater.getSpoutMetrics();

        int retry = 5;
//        double avgAckedRate = outSpoutMetrics.getAckedRate();
//        int counter = 1;
        double avgAckedRate = 0;
        int counter = 0;

        while(true) {
            spoutMetricsUpdater.updateMetrics();

            spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();

            avgAckedRate += spoutMetrics.getAckedRate();
            counter++;

            if(spoutMetrics.getAckedRate() > outSpoutMetrics.getAckedRate()) {
                if ( (spoutMetrics.getAckedRate() - outSpoutMetrics.getAckedRate()) / outSpoutMetrics.getAckedRate() < 0.05 )
                    retry--;

                outSpoutMetrics = spoutMetrics;
            }
            else {
                retry--;
            }

            if (retry == 0)
                break;

            Thread.sleep(60*1000);

        }

        writeData(outSpoutMetrics, avgAckedRate/counter);

    }

    public static void main(String args[]) {
        try {

            TopologySubmitter2 t = new TopologySubmitter2("testWC");

            for (int i = 1; i <= 4; i++) { //worker
                for (int j = 1; j <= i; j++) { //spout
                    for (int k = 1; k <= 2 * i; k++) { //split
                        for (int w = 1; w <= 2 * i; w++) { //count
                            System.out.println("Submitting topology : (" + i + "," + j + "," + k + "," + w + ") " + "Waiting for 30 sec");
                            t.submitWCTopology(i, j, k, w);

                            Thread.sleep(30 * 1000);
                            t.monitorTopology();

                            System.out.println("Killing topology : (" + i + "," + k + "," + j + "," + w + ") " + "Waiting for 20 sec");
                            t.killTopology();
                            Thread.sleep(20 * 1000);
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
