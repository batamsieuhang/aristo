package org.apache.storm.starter.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by vgol on 31/1/2017.
 */
public class AggregatorNumberBolt extends BaseBasicBolt {
    private Queue<Integer> queue1;
    private Queue<Integer> queue2;

    public AggregatorNumberBolt() {
        queue1 = new LinkedList<>();
        queue2 = new LinkedList<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer input = tuple.getInteger(0);
        //We can use: tuple.getSourceStreamId().equalsIgnoreCase("#streamId#") if we want streams instead of components
        if(tuple.getSourceStreamId().equalsIgnoreCase("0"))
        //if(tuple.getSourceComponent().equalsIgnoreCase("spout"))
        {
            if(queue2.isEmpty())
                queue1.add(input);
            else
                collector.emit(new Values(queue2.poll() + input));

        }
        else if(tuple.getSourceStreamId().equalsIgnoreCase("1"))
//        else if(tuple.getSourceComponent().equalsIgnoreCase("general"))
        {
            if(queue1.isEmpty())
                queue2.add(input);
            else
                collector.emit(new Values(queue1.poll() + input));

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer"));
    }
}


