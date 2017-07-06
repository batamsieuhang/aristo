package org.apache.storm.starter.bolt;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MatrixInverterBolt extends BaseBasicBolt {

    Array2DRowRealMatrix input;
    Integer n;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        input = (Array2DRowRealMatrix) tuple.getValue(0);
        n = input.getRowDimension();

        double[][] temp = new double[n][n];

        for(int i=0;i<n;i++)
            temp[i][i] = 1;

        Array2DRowRealMatrix I = new Array2DRowRealMatrix(temp);

        LUDecomposition lu = new LUDecomposition(input);

        Array2DRowRealMatrix output = (Array2DRowRealMatrix) lu.getSolver().solve(I);

        collector.emit(new Values(output));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("matrix"));
    }
}
