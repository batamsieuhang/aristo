package org.apache.storm.starter.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vgol on 31/1/2017.
 */
public class MultipleStreamNumberBolt extends BaseBasicBolt {
    List<String> streamNames;
    int streamCount;

    public MultipleStreamNumberBolt() {
        streamCount = 1;
        streamNames = new ArrayList<>();
        streamNames.add("0");
    }

    public MultipleStreamNumberBolt(int streamCount) {
        this.streamCount = streamCount;
        streamNames = new ArrayList<>();
        for(int i=0; i<streamCount; i++)
            streamNames.add(Integer.toString(i));
    }

    public MultipleStreamNumberBolt(int streamCount, List<String> streamNames) {
        this.streamCount = streamCount;
        if(streamNames.size() != streamCount)
            throw new IllegalArgumentException("Size of stream names array does not match with stream count");

        this.streamNames = streamNames;
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer n = tuple.getInteger(0);

        for(int i=0; i<streamCount; i++)
            collector.emit(streamNames.get(i), new Values(n));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(int i=0; i<streamCount; i++)
            declarer.declareStream(streamNames.get(i), new Fields("integer"));
    }
}
