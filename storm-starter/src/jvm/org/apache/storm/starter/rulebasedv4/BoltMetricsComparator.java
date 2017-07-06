package org.apache.storm.starter.rulebasedv4;

import org.apache.storm.starter.metrics.BoltMetrics;
import org.apache.storm.starter.metrics.BoltMetricsUpdater;
import org.apache.storm.starter.rulebased.ComponentNode;

import java.util.Comparator;

public class BoltMetricsComparator implements Comparator<ComponentNode>{

    @Override
    public int compare(ComponentNode x, ComponentNode y) throws NullPointerException{
        BoltMetrics boltStatsX = ((BoltMetricsUpdater)x.getNode().getComponentUpdater()).getBoltMetrics();
        BoltMetrics boltStatsY = ((BoltMetricsUpdater)y.getNode().getComponentUpdater()).getBoltMetrics();

        //We want descending order so that's why we return -1 if capacityX > capacityY
        if(boltStatsX.getMaxCapacity() > boltStatsY.getMaxCapacity())
            return -1;

        if(boltStatsX.getMaxCapacity() < boltStatsY.getMaxCapacity())
            return 1;

        return 0;
    }

}
