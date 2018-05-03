package stormapplied.githubcommits.topology;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import stormapplied.githubcommits.datasource.MockedQueue;

import java.util.Map;

/**
 * This spout simulates reading queue from a live stream by doing two things:
 * <p/>
 * 1) Reading a file containing commit data into a list of strings (one string per commit).
 * 2) When nextTuple() is called, emit a tuple for each string in the list.
 */
public class CommitFeedListener extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private MockedQueue queue = new MockedQueue();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("commit"));
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void nextTuple() {
        String commit = queue.getMessage();
        outputCollector.emit(new Values(commit));
    }
}