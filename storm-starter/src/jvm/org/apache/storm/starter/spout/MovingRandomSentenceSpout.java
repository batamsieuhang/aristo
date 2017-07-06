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

public class MovingRandomSentenceSpout extends BaseRichSpout {

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
    long _minRatePerSecond;
    long _maxRatePerSecond;
    long _stepRatePerSecond;
    int direction; //-1 = down, 1 = up
    Random _rand;
    long _nextEmitTime;
    long _nextStepTime;
    long _stepNano;


    public MovingRandomSentenceSpout(long minRatePerSecond, long maxRatePerSecond, long stepRatePerSecond, long stepPeriod) {
        _minRatePerSecond = minRatePerSecond;
        _maxRatePerSecond = maxRatePerSecond;
        _stepRatePerSecond = stepRatePerSecond;

        _stepNano = stepPeriod*1000000000;

    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();

        _currentRatePerSecond = _minRatePerSecond+_stepRatePerSecond;
        _periodNano = Math.max(1, 1000000000/_currentRatePerSecond);
        direction = 1;
        _nextEmitTime = System.nanoTime();
        _nextStepTime = System.nanoTime() + _stepNano;
    }

    @Override
    public void nextTuple() {

        if (_nextStepTime <= System.nanoTime()){
            _currentRatePerSecond = _currentRatePerSecond + direction*_stepRatePerSecond;

            if(_currentRatePerSecond <= _minRatePerSecond || _currentRatePerSecond >= _maxRatePerSecond)  //care, if it is strted from min or max then we have a problem, especially when min and step are the same then divide by  in 65
                direction = -direction;

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