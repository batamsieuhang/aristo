package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by vgol on 31/1/2017.
 */
public class MultipleStreamSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    ThreadLocalRandom _rand;
    int _min, _max;
    List<String> streamNames;
    int streamCount;

    public MultipleStreamSpout() {
        streamCount = 1;
        streamNames = new ArrayList<>();
        streamNames.add("0");
    }

    public MultipleStreamSpout(int streamCount) {
        this.streamCount = streamCount;
        streamNames = new ArrayList<>();
        for(int i=0; i<streamCount; i++)
            streamNames.add(Integer.toString(i));
    }

    public MultipleStreamSpout(int streamCount, List<String> streamNames) {
        this.streamCount = streamCount;
        if(streamNames.size() != streamCount)
            throw new IllegalArgumentException("Size of stream names array does not match with stream count");

        this.streamNames = streamNames;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();
    }

    @Override
    public void nextTuple() {
        Integer number = _rand.nextInt(1, 10);
        for(int i=0; i<streamCount; i++)
            _collector.emit(streamNames.get(i), new Values(number));
    }

    @Override
    public void ack(Object id) {
        //Ignored
    }

    @Override
    public void fail(Object id) {
//        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(int i=0; i<streamCount; i++)
            declarer.declareStream(streamNames.get(i), new Fields("integer"));
    }
}
