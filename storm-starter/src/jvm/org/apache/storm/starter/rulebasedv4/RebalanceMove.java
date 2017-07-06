package org.apache.storm.starter.rulebasedv4;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class RebalanceMove {

    private RebalanceOptions options;
    private Map<String, Integer> components;

    public RebalanceMove() {
        options = new RebalanceOptions();
        options.set_wait_secs(0);
        components = new HashMap();
    }

    public void addComponent(String componentName, Integer parallelism) {
        components.put(componentName, parallelism);
    }

    public void setWorkers(Integer workers) {
        options.set_num_workers(workers);
    }

    public void commitRebalance(String topologyName) throws Exception{
        Map clusterConf = Utils.readStormConfig();
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();


        options.set_num_executors(components);

        client.rebalance(topologyName, options);
    }


    public Map<String, Integer> getComponents() {
        return components;
    }


    public static void main() throws Exception{
        RebalanceMove rb = new RebalanceMove();
        rb.addComponent("SplitSentence", new Integer(2));
        rb.setWorkers(1);
        rb.commitRebalance("test");
    }
}
