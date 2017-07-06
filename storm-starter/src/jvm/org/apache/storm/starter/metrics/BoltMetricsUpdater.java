package org.apache.storm.starter.metrics;

import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ExecutorAggregateStats;

public class BoltMetricsUpdater implements ComponentMetricsUpdaterInterface{

    private BoltMetrics boltMetrics;
    private ComponentMetricsCreator component;

    public BoltMetricsUpdater(ComponentMetricsCreator component) throws Exception{
        if (component.getComponentType() != 1)
            throw  new Exception("Illegal component type " + component.getComponentType());

        this.component = component;
        boltMetrics = new BoltMetrics(component.getComponentId());

    }

    public void updateMetrics() {
        try {
            ComponentPageInfo componentPage = component.getClient().getComponentPageInfo(component.getTopologyId(), component.getComponentId(), ":all-time", false);
            boltMetrics.setExecutors(componentPage.get_num_executors());
            boltMetrics.setTasks(componentPage.get_num_tasks());


            int uptime = component.getClient().getTopologyInfo(component.getTopologyId()).get_uptime_secs();
            long totalAcked = 0;
            long totalFailed = 0;
            long totalEmitted = 0;
            long totalTransfered = 0;
            long totalExecuted = 0;
            double avgProcessLatency = 0;
            double avgExecuteLatency = 0;
            double capacity = 0;
            double maxCapacity = 0;

            for (ExecutorAggregateStats stats: componentPage.get_exec_stats()) {
                CommonAggregateStats common = stats.get_stats().get_common_stats();
                BoltAggregateStats specific = stats.get_stats().get_specific_stats().get_bolt();

                totalAcked += common.get_acked();
                totalFailed += common.get_failed();
                totalEmitted += common.get_emitted();
                totalTransfered += common.get_transferred();
                totalExecuted += specific.get_executed();

                avgProcessLatency += specific.get_process_latency_ms()*common.get_acked();
                avgExecuteLatency += specific.get_execute_latency_ms()*specific.get_executed();

                capacity += specific.get_capacity();
                maxCapacity = Math.max(maxCapacity, specific.get_capacity());

            }

            boltMetrics.setCapacity(capacity/boltMetrics.getExecutors());
            boltMetrics.setMaxCapacity(maxCapacity);

            boltMetrics.setAckedRate((double)(totalAcked - boltMetrics.getAcked()) / (uptime - boltMetrics.getUptime()));
            boltMetrics.setEmitRate((double)(totalEmitted - boltMetrics.getEmitted()) / (uptime - boltMetrics.getUptime()));
            boltMetrics.setTransferRate((double)(totalTransfered - boltMetrics.getTransfered()) / (uptime - boltMetrics.getUptime()));
            boltMetrics.setExecuteRate((double)(totalExecuted - boltMetrics.getExecuted()) / (uptime - boltMetrics.getUptime()));

            boltMetrics.setProcessLatency(avgProcessLatency / totalAcked);
            boltMetrics.setExecuteLatency(avgExecuteLatency / totalExecuted);

            boltMetrics.setUptime(uptime);
            boltMetrics.setAcked(totalAcked);
            boltMetrics.setFailed(totalFailed);
            boltMetrics.setEmitted(totalEmitted);
            boltMetrics.setTransfered(totalTransfered);
            boltMetrics.setExecuted(totalExecuted);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void printMetrics() {
        boltMetrics.PrintSpoutMetrics();
    }

    public BoltMetrics getBoltMetrics() {
        return boltMetrics;
    }

    public ComponentMetricsCreator getComponent() {
        return component;
    }
}
