package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class ControlledRandomSentenceSpout extends BaseRichSpout {

    private static Map<Object , Integer> cache = new HashMap();
    private static final String[] CHOICES = {
            "marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"
    };

    SpoutOutputCollector _collector;
    long _periodNano;
    long _emitAmount;
    Random _rand;
    long _nextEmitTime;


    public ControlledRandomSentenceSpout(long ratePerSecond) {
        if (ratePerSecond > 0) {
            _periodNano = Math.max(1, 1000000000/ratePerSecond);
        } else {
            _periodNano = Long.MAX_VALUE - 1;
        }
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();
        _nextEmitTime = System.nanoTime();
    }

    @Override
    public void nextTuple() {
        if (_nextEmitTime <= System.nanoTime()) {
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            _collector.emit(new Values(sentence), sentence);
            _nextEmitTime = _nextEmitTime + _periodNano;
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
        if (cache.containsKey(id)) {
            int replays = cache.get(id);
            if (replays++ <5 ) {
                cache.put(id, replays);
                _collector.emit(new Values(id), id);
            }
            else
                cache.remove(id);

        }
        else {
            cache.put(id, 1);
            _collector.emit(new Values(id), id);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}