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

//import org.json.*;

public class TopologyCheck {


	public static void checkConfig(Nimbus.Client client, Map clusterConf) throws Exception {


		String id = null;
		if (clusterConf == null)
			throw new Exception("Could not find Cluster configurations");

		ClusterSummary summary = client.getClusterInfo();

		for (TopologySummary ts: summary.get_topologies()) {
			TopologyPageInfo tpi = client.getTopologyPageInfo(ts.get_id(), ":all-time", false);
			//System.out.println(tpi.get_topology_conf());
			//its ok apla den exw to library sto intellij
			/*JSONObject conf = new JSONObject(tpi.get_topology_conf());
			String gg = conf.getString(ts.get_name());
			System.out.println(gg);
			*/
		}
	}

	public static void main(String[] args) throws Exception{

		Map clusterConf = Utils.readStormConfig();
		Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
		checkConfig(client, clusterConf);
	}
}