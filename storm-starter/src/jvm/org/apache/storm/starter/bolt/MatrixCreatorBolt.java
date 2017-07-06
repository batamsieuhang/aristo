package org.apache.storm.starter.bolt;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MatrixCreatorBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer n = tuple.getInteger(0);

        double[][] M = new double[n][n];

        int a = (n+1)/2;
        int b = (n+1);
        for (int j = 0; j < n; j++) {
            for (int i = 0; i < n; i++) {
                M[i][j] = n*((i+j+a) % n) + ((i+2*j+b) % n) + 1;
            }
        }

        Array2DRowRealMatrix output = new Array2DRowRealMatrix(M);

        collector.emit(new Values(output));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("matrix"));
    }
}
