package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MatrixSizeGeneratorSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    ThreadLocalRandom _rand;
    int _min, _max;

    public MatrixSizeGeneratorSpout() {
        _min = 2;
        _max = 10;
    }

    public MatrixSizeGeneratorSpout(int minSize, int maxSize) {
        _min = minSize;
        _max = maxSize;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();
    }

    @Override
    public void nextTuple() {
        Integer matrixSize = _rand.nextInt(_min, _max);
        _collector.emit(new Values(matrixSize), matrixSize);
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
        declarer.declare(new Fields("size"));
    }
}
