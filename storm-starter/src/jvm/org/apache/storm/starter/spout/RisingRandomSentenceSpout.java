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

public class RisingRandomSentenceSpout extends BaseRichSpout {

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
    long _currentRatePerSecond;
    long _startRatePerSecond;
    long _stopRatePerSecond;
    long _stepRatePerSecond;
    Random _rand;
    long _nextEmitTime;
    long _nextStepTime;
    long _stepNano;


    public RisingRandomSentenceSpout(long startRatePerSecond, long stopRatePerSecond, long stepRatePerSecond, long stepPeriod) {
        _startRatePerSecond = startRatePerSecond;
        _stopRatePerSecond = stopRatePerSecond;
        _stepRatePerSecond = stepRatePerSecond;

        _stepNano = stepPeriod*1000000000;

    }

    public RisingRandomSentenceSpout(long stepRatePerSecond, long stepPeriod) {
        _startRatePerSecond = 0;
        _stopRatePerSecond = 0;
        _stepRatePerSecond = stepRatePerSecond;

        _stepNano = stepPeriod*1000000000;

    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();

        _currentRatePerSecond = _startRatePerSecond + _stepRatePerSecond;
        _periodNano = Math.max(1, 1000000000/_currentRatePerSecond);
        _nextEmitTime = System.nanoTime();
        _nextStepTime = System.nanoTime() + _stepNano;
    }

    @Override
    public void nextTuple() {

        if (_nextStepTime <= System.nanoTime() && (_stepRatePerSecond == 0 || _stopRatePerSecond < _currentRatePerSecond)){
            _currentRatePerSecond = _currentRatePerSecond + _stepRatePerSecond;

            _periodNano = Math.max(1, 1000000000/_currentRatePerSecond);
            _nextStepTime = _nextStepTime + _stepNano;
        }




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

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }


}
