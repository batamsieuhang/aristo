package org.apache.storm.starter.custom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;


public class Rebalance {


    public static void TopoRebalance(Nimbus.Client client, String name) throws Exception {

        RebalanceOptions ro = new RebalanceOptions();
        ro.set_num_workers(2);
        Map<String, Integer> m = new HashMap();
        m.put("split", 8);
        m.put("count", 8);
        ro.set_num_executors(m);
        ro.set_wait_secs(0);

        client.rebalance(name,ro);
    }

    public static void main(String[] args) throws Exception{

        Map clusterConf = Utils.readStormConfig();
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        TopoRebalance(client, "wc-test");
    }
}