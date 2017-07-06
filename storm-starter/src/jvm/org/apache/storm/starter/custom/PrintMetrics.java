package org.apache.storm.starter.custom;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;


public class PrintMetrics {

    private static long lastAcked = 0;


    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        try{
            for (ExecutorSummary exec: info.get_executors()) {
                if ("spout".equals(exec.get_component_id())) {
                    SpoutStats stats = exec.get_stats().get_specific().get_spout();
                    Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                    Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                    Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                    for (String key: ackedMap.keySet()) {
                        if (failedMap != null) {
                            Long tmp = failedMap.get(key);
                            if (tmp != null) {
                                failed += tmp;
                            }
                        }
                        long ackVal = ackedMap.get(key);
                        double latVal = avgLatMap.get(key) * ackVal;
                        //System.out.println(avgLatMap.get(key));
                        acked += ackVal;
                        weightedAvgTotal += latVal;
                    }
                }
            }
            double avgLatency = weightedAvgTotal/acked;
            System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed) + " throughput: " + ((double)acked-(double)lastAcked)/10);
            lastAcked = acked;
        }
        catch(NullPointerException e) {
            e.printStackTrace();
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }


    public static void componentMetrics(Nimbus.Client client, String topology, String component) throws Exception{
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (topology.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+topology);
        }

        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();


        try {
            ComponentPageInfo componentPage = client.getComponentPageInfo(id,component,":all-time",false);
            if (componentPage.get_component_type().getValue()==1) {
                System.out.println("\nBolt : " + component);
                int executorsNumber = componentPage.get_num_executors();
                System.out.println("Executors number: " + executorsNumber);
                System.out.println("Tasks number: " + componentPage.get_num_tasks() + "\n");
                System.out.println("Executor Stats");

                long totalExecuted = 0;
                long totalAcked = 0;
                long totalFailed = 0;
                double avgProcessLatency = 0;
                double avgExecuteLatency = 0;
                double avgCapacity = 0;
                for (ExecutorAggregateStats stats: componentPage.get_exec_stats()) {
                    CommonAggregateStats common = stats.get_stats().get_common_stats();
                    BoltAggregateStats specific = stats.get_stats().get_specific_stats().get_bolt();

                    long acked = common.get_acked();
                    long failed = common.get_failed();
                    long executed = specific.get_executed();
                    double capacity = specific.get_capacity();
                    double executeLatency = specific.get_execute_latency_ms();
                    double processLatency = specific.get_process_latency_ms();

                    totalExecuted += executed;
                    totalAcked += acked;
                    totalFailed += failed;
                    avgProcessLatency += processLatency*acked;
                    avgExecuteLatency += executeLatency*executed;
                    avgCapacity += capacity/executorsNumber;

                    System.out.println("acked: " + acked + "  failed: " + failed + "  process latency: " + processLatency + "  executed: " + executed + "  execute latency: " + executeLatency + "  capacity: " + capacity);

                }

                avgProcessLatency /= totalAcked;
                avgExecuteLatency /= totalExecuted;
                double throughput = ((double)totalAcked)/uptime;

                System.out.println("Total stats");
                System.out.println("uptime: " + uptime + "  acked: " + totalAcked + "  failed: " + totalFailed + "  throughput: " + throughput + "  process latency: " + avgProcessLatency + "  executed: " + totalExecuted + "  execute latency: " + avgExecuteLatency + "  capacity: " + avgCapacity);


            }
            else if (componentPage.get_component_type().getValue()==2) {
                System.out.println("\nSpout : " + component);
                int executorsNumber = componentPage.get_num_executors();
                System.out.println("Executors number: " + executorsNumber);
                System.out.println("Tasks number: " + componentPage.get_num_tasks() + "\n");
                System.out.println("Executor Stats");

                long totalAcked = 0;
                long totalFailed = 0;
                double avgLatency = 0;

                for (ExecutorAggregateStats stats: componentPage.get_exec_stats()) {
                    CommonAggregateStats common = stats.get_stats().get_common_stats();
                    SpoutAggregateStats specific = stats.get_stats().get_specific_stats().get_spout();

                    long acked = common.get_acked();
                    long failed = common.get_failed();
                    double latency = specific.get_complete_latency_ms();

                    totalAcked += acked;
                    totalFailed += failed;
                    avgLatency += latency*acked;

                    System.out.println("acked: " + acked + "  failed: " + failed + "  complete latency: " + latency);
                }

                avgLatency /= totalAcked;
                double throughput = ((double)totalAcked)/uptime;

                System.out.println("uptime: " + uptime + "  acked: " + totalAcked + "  failed: " + totalFailed + "  throughput: " + throughput + "  latency: " + avgLatency);
            }



        }
        catch(NullPointerException e) {
            e.printStackTrace();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception{


        String name = "wc-test";
        if (args != null && args.length > 0) {
            name = args[0];
        }


        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        for (int i = 0; i < 1000; i++) {

            printMetrics(client, name);

            //componentMetrics(client, name, "spout");
            //componentMetrics(client, name, "split");
            //componentMetrics(client, name, "count");

            Thread.sleep(10 * 1000);
        }
    }
}