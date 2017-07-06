package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by VagelisAkis on 9/2/2017.
 */
public class Topo {

    public static void main(String args[]) {
        try {
            Config conf = new Config();
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            conf.setNumWorkers(2);

            topologyBuilder.setSpout("blue-spout", new BlueSpout(), 2);

            topologyBuilder.setBolt("green-bolt", new GreenBolt(), 2)
                    .setNumTasks(4)
                    .shuffleGrouping("blue-spout");

            topologyBuilder.setBolt("yellow-bolt", new YellowBolt(), 6)
                    .shuffleGrouping("green-bolt");

            StormSubmitter.submitTopology("mytopology", conf,
                    topologyBuilder.createTopology());
        }
        catch (Exception e){

        }
    }
}
