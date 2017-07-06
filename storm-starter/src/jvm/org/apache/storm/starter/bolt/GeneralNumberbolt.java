package org.apache.storm.starter.bolt;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by vgol on 31/1/2017.
 */
public class GeneralNumberbolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer n = tuple.getInteger(0);

        collector.emit(new Values(n));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer"));
    }
}
