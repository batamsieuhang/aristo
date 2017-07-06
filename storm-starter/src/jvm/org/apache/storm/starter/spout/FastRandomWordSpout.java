package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class FastRandomWordSpout extends BaseRichSpout{

    SpoutOutputCollector _collector;
    Random _rand;
//    private static final String[] WORDS = {
//            "marry", "had", "a", "little", "lamb", "whos", "fleese", "was", "white", "as", "snow",
//            "and", "every", "where", "that", "marry", "went", "the", "lamb", "was", "sure", "to", "go",
//            "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
//            "this", "is", "a", "test", "of", "the", "emergency", "broadcast", "system", "this", "is", "only", "a", "test",
//            "peter", "piper", "picked", "a", "peck", "of", "pickeled", "peppers"
//    };

    private static final String[] SENTENCES = {
            "marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"
    };

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = ThreadLocalRandom.current();
    }

//    @Override
//    public void nextTuple() {
//        String word = WORDS[_rand.nextInt(WORDS.length)];
//        _collector.emit(new Values(word), word);
//    }

    @Override
    public void nextTuple() {
        String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
        for (String word: sentence.split("\\s+")) {
//            _collector.emit(new Values(word), word);
            _collector.emit(new Values(word));
        }
    }

    @Override
    public void ack(Object id) {
        //Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
