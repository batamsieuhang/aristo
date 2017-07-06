package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.bolt.SplitSentence;
import org.apache.storm.starter.bolt.WordCount;
import org.apache.storm.starter.spout.FastRandomSentenceSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by VagelisAkis on 4/12/2016.
 */
public class FastWordCountTopology2 {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FastRandomSentenceSpout(), Integer.parseInt(args[2]));

        builder.setBolt("split", new SplitSentence(), Integer.parseInt(args[3])).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), Integer.parseInt(args[4])).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();

        String name = "wc-test";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(Integer.parseInt(args[1]));
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());

    }
}
