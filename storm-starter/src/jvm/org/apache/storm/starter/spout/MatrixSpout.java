package org.apache.storm.starter.spout;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MatrixSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    ThreadLocalRandom _rand;
    int _min, _max;
    Integer n;

    public MatrixSpout() {
        _min = 2;
        _max = 3;
    }

    public MatrixSpout(int minSize, int maxSize) {
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
        n = _rand.nextInt(_min, _max);

        double[][] M = new double[n][n];

        int a = (n+1)/2;
        int b = (n+1);
        for (int j = 0; j < n; j++) {
            for (int i = 0; i < n; i++) {
                M[i][j] = n*((i+j+a) % n) + ((i+2*j+b) % n) + 1;
            }
        }

        Array2DRowRealMatrix output = new Array2DRowRealMatrix(M);

        _collector.emit(new Values(output), output);
//        _collector.emit(new Values(output));
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
        declarer.declare(new Fields("matrix"));
    }
}
