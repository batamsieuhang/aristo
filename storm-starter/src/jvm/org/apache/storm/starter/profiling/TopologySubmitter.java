package org.apache.storm.starter.profiling;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.WordCountTopology;
import org.apache.storm.starter.bolt.AggregatorNumberBolt;
import org.apache.storm.starter.bolt.MultipleStreamNumberBolt;
import org.apache.storm.starter.metrics.*;
import org.apache.storm.starter.spout.MultipleStreamSpout;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by vgol on 24/9/2016.
 */
public class TopologySubmitter {

    private String topologyName;
    private String spoutId;
    private String boltId;

    private Class spoutClass;
    private Class boltClass;

    private static final String packagePath = "org.apache.storm.starter";
    private static final String spoutPath = ".spout.";
    private static final String boltPath = ".bolt.";

    private String boltGrouping;
    private String groupingId;
    //private String streamId;

    private TopologyBuilder builder;

    private OutputToARFF out;
    private int workers;


    public TopologySubmitter(String topologyName, String spoutId, String boltId, String boltGrouping) throws ClassNotFoundException, IOException{
        this.topologyName = topologyName;
        this.spoutId = spoutId;
        this.boltId = boltId;

        spoutClass = Class.forName(packagePath+spoutPath+spoutId);
        boltClass = Class.forName(packagePath+boltPath+boltId);

        this.boltGrouping = boltGrouping;

        out = new OutputToARFF("", boltId + ".txt");
        out.setRelation(boltId);
        out.initializeFileForStorm();
    }

    public void setGroupingId(String groupingId) {
        this.groupingId = groupingId;
    }

    public TopologyBuilder getTopologyBuilder() {
        return builder;
    }


    public void buildTopology(int spoutExecutors, int boltExecutors) throws Exception{
        builder = new TopologyBuilder();

        builder.setSpout(spoutId, (IRichSpout) spoutClass.newInstance(), spoutExecutors);


        BoltDeclarer boltDeclarer = builder.setBolt(boltId, (BaseBasicBolt) boltClass.newInstance(), boltExecutors);
        //boltDeclarer.setNumTasks(4);
        //BoltDeclarer boltDeclarer = builder.setBolt(boltId, (IRichBolt) boltClass.newInstance(), boltExecutors);

        if(groupingId != null){
            Method groupingMethod = boltDeclarer.getClass().getMethod(boltGrouping, String.class, Fields.class);
            groupingMethod.invoke(boltDeclarer, spoutId, new Fields(groupingId));
        }
        else{
            Method groupingMethod = boltDeclarer.getClass().getMethod(boltGrouping, String.class);
            groupingMethod.invoke(boltDeclarer,spoutId);
        }

    }

    public void buildCustomTopology(int spoutExecutors, int boltExecutors) throws Exception{
        builder = new TopologyBuilder();

        //GeneralNumberBolt
//        builder.setSpout(spoutId, (IRichSpout) spoutClass.newInstance(), spoutExecutors);
//        builder.setBolt(boltId, (BaseBasicBolt) boltClass.newInstance(), boltExecutors).shuffleGrouping(spoutId,"0");

        //MultipleStreamNumberBolt (distribute -> 2 out streams)
        builder.setSpout(spoutId, new MultipleStreamSpout(), spoutExecutors);
        builder.setBolt(boltId, new MultipleStreamNumberBolt(2), boltExecutors).shuffleGrouping(spoutId,"0");

        //AggregatorNumberBolt (spout with 2 out streams)
//        builder.setSpout(spoutId, new MultipleStreamSpout(2), spoutExecutors);
//        builder.setBolt(boltId, new AggregatorNumberBolt(), boltExecutors).shuffleGrouping(spoutId,"0").shuffleGrouping(spoutId,"1");
    }

    public void submitTopologyCluster(int workers) throws Exception{
        this.workers = workers;

        Config conf = new Config();
        conf.setDebug(false);

        conf.setNumWorkers(workers);

        //conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
        //conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
        //conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, 0.95);
        //conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, 0.8);
        //conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);


        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }

    public void submitTopologyLocal() {
        Config conf = new Config();
        conf.setDebug(false);

        conf.setMaxTaskParallelism(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
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

        ComponentMetricsCreator spout = new ComponentMetricsCreator(topologyName, spoutId);
        ComponentMetricsCreator bolt = new ComponentMetricsCreator(topologyName, boltId);

        SpoutMetricsUpdater spoutMetricsUpdater = (SpoutMetricsUpdater)spout.getComponentUpdater();
        BoltMetricsUpdater boltMetricsUpdater = (BoltMetricsUpdater)bolt.getComponentUpdater();

        spoutMetricsUpdater.updateMetrics();
        boltMetricsUpdater.updateMetrics();
        Thread.sleep(20*1000);

        SpoutMetrics spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
        BoltMetrics boltMetrics = boltMetricsUpdater.getBoltMetrics();

        SpoutMetrics outSpoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
        BoltMetrics outBoltMetrics = boltMetricsUpdater.getBoltMetrics();

        int retry = 5;

        while(true) {
            spoutMetricsUpdater.updateMetrics();
            boltMetricsUpdater.updateMetrics();

            spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
            boltMetrics = boltMetricsUpdater.getBoltMetrics();

            if(spoutMetrics.getAckedRate() > outSpoutMetrics.getAckedRate()) {
                if ( (spoutMetrics.getAckedRate() - outSpoutMetrics.getAckedRate()) / outSpoutMetrics.getAckedRate() < 0.05 )
                    retry--;

                outSpoutMetrics = spoutMetrics;
                outBoltMetrics = boltMetrics;
            }
            else {
                retry--;
            }

            if (retry == 0)
                break;

            Thread.sleep(60*1000);

        }

        System.out.println("Writing output");
        out.writeData(workers, outSpoutMetrics, outBoltMetrics);

    }

    private void monitorTopologyNA() throws Exception{

        ComponentMetricsCreator spout = new ComponentMetricsCreator(topologyName, spoutId);
        ComponentMetricsCreator bolt = new ComponentMetricsCreator(topologyName, boltId);

        SpoutMetricsUpdater spoutMetricsUpdater = (SpoutMetricsUpdater)spout.getComponentUpdater();
        BoltMetricsUpdater boltMetricsUpdater = (BoltMetricsUpdater)bolt.getComponentUpdater();

        spoutMetricsUpdater.updateMetrics();
        boltMetricsUpdater.updateMetrics();
        Thread.sleep(20*1000);

        SpoutMetrics spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
        BoltMetrics boltMetrics = boltMetricsUpdater.getBoltMetrics();

        SpoutMetrics outSpoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
        BoltMetrics outBoltMetrics = boltMetricsUpdater.getBoltMetrics();

        int retry = 5;

        while(true) {
            spoutMetricsUpdater.updateMetrics();
            boltMetricsUpdater.updateMetrics();

            spoutMetrics = spoutMetricsUpdater.getSpoutMetrics();
            boltMetrics = boltMetricsUpdater.getBoltMetrics();

            if(boltMetrics.getAckedRate() > outBoltMetrics.getAckedRate()) {
                if ( (boltMetrics.getAckedRate() - outBoltMetrics.getAckedRate()) / outBoltMetrics.getAckedRate() < 0.05 )
                    retry--;

                outSpoutMetrics = spoutMetrics;
                outBoltMetrics = boltMetrics;
            }
            else {
                retry--;
            }

            if (retry == 0)
                break;

            Thread.sleep(60*1000);

        }

        System.out.println("Writing output");
        out.writeDataNA(workers, outSpoutMetrics, outBoltMetrics);

    }


    public static void main(String args[]) {
        try {
//            t.buildTopology(Integer.parseInt(args[1]),Integer.parseInt(args[2]));
//            t.submitTopologyCluster(Integer.parseInt(args[0]));


/*            TopologySubmitter t = new TopologySubmitter("test", "FastRandomSentenceSpout", "SplitSentence", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<i+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopology();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/


            /*TopologySubmitter t = new TopologySubmitter("test", "FastRandomWordSpout", "WordCount", "fieldsGrouping");
            t.setGroupingId("word");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopology();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/


            /*TopologySubmitter t = new TopologySubmitter("test", "MatrixSizeGeneratorSpout", "MatrixCreatorBolt", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=j; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopology();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/

            /*t = new TopologySubmitter("test", "MatrixSpout", "MatrixInverterBolt", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopology();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/

            TopologySubmitter t = new TopologySubmitter("test", "MultipleStreamSpout", "MultipleStreamNumberBolt", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildCustomTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopologyNA();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }

            /*TopologySubmitter t = new TopologySubmitter("test", "MultipleStreamSpout", "GeneralNumberbolt", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildCustomTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopologyNA();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/

            /*TopologySubmitter t = new TopologySubmitter("test", "MultipleStreamSpout", "AggregatorNumberBolt", "shuffleGrouping");

            for(int i=1; i<5; i++) { //workers
                for(int j=1; j<2*i+1; j++) { //bolts
                    for(int k=1; k<2*j+1; k++) { //spouts

                        System.out.println("Submitting topology : (" + i +"," + k +"," + j + ") " + "Waiting for 30 sec");
                        t.buildCustomTopology(k, j);
                        t.submitTopologyCluster(i);

                        Thread.sleep(30*1000);
                        t.monitorTopologyNA();

                        System.out.println("Killing topology : (" + i +"," + k +"," + j + ") " + "Waiting for 20 sec");
                        t.killTopology();
                        Thread.sleep(20*1000);
                    }
                }
            }*/


        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
