package org.apache.storm.starter.profiling;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.bolt.MatrixCreatorBolt;
import org.apache.storm.starter.bolt.MatrixInverterBolt;
import org.apache.storm.starter.metrics.*;
import org.apache.storm.starter.spout.MatrixSizeGeneratorSpout;
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
public class TopologySubmitter3 {

    private String topologyName;
    private File file;


    public TopologySubmitter3(String topologyName){
        this.topologyName = topologyName;
        file = new File(topologyName + ".txt");
    }

    public void writeData(SpoutMetrics spoutMetrics, double avgAckedRate) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        writer.println(avgAckedRate + " , " + spoutMetrics.getAckedRate() + " , " + spoutMetrics.getCompleteLatency());
        writer.close();
    }

    public void writeData(int workers, int spout, int matrix, int inverse)throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";
        writer.println(workers + comma + spout + comma + matrix + comma + inverse);
        writer.close();
    }

    public void submitTopology(int workers, int spout, int matrix, int inverse) throws Exception{
        writeData(workers, spout, matrix, inverse);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MatrixSizeGeneratorSpout(), spout);

        builder.setBolt("matrix", new MatrixCreatorBolt(),matrix).shuffleGrouping("spout");
        builder.setBolt("inverse", new MatrixInverterBolt(), inverse).shuffleGrouping("matrix");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 60000);
        conf.registerSerialization(Array2DRowRealMatrix.class);

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
        Thread.sleep(40*1000);

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

            TopologySubmitter3 t = new TopologySubmitter3("testMX");

            for (int i = 1; i <= 4; i++) { //worker
                for (int j = 1; j <= i; j++) { //spout
                    for (int k = 1; k <= i; k++) { //matrix
                        for (int w = 1; w <= i; w++) { //inverse
                            System.out.println("Submitting topology : (" + i + "," + j + "," + k + "," + w + ") " + "Waiting for 30 sec");
                            t.submitTopology(i, j, k, w);

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
