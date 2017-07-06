package org.apache.storm.starter;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bolt.AggregatorNumberBolt;
import org.apache.storm.starter.bolt.GeneralNumberbolt;
import org.apache.storm.starter.bolt.MatrixCreatorBolt;
import org.apache.storm.starter.bolt.MatrixInverterBolt;
import org.apache.storm.starter.spout.MatrixSizeGeneratorSpout;
import org.apache.storm.starter.spout.MultipleStreamSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by vgol on 31/1/2017.
 */
public class MontageTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MultipleStreamSpout(2), Integer.parseInt(args[2]));

        builder.setBolt("general", new GeneralNumberbolt(), Integer.parseInt(args[3])).shuffleGrouping("spout", "0");
        builder.setBolt("aggregate", new AggregatorNumberBolt(), Integer.parseInt(args[4])).shuffleGrouping("general").shuffleGrouping("spout","1");
        builder.setBolt("general2", new GeneralNumberbolt(), Integer.parseInt(args[5])).shuffleGrouping("aggregate");

        Config conf = new Config();

//        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 60000);
//        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
//        conf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 1);
//        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
//        conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, 0.9);
//        conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, 0.5);

        String name = "matrix-test";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(Integer.parseInt(args[1]));
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());

    }
}
