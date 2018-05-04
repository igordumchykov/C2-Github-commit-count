package stormapplied.githubcommits.topology;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import stormapplied.githubcommits.datasource.MockedQueue;

import java.util.Map;

/**
 * This spout simulates reading queue from a live stream by doing two things:
 * <p/>
 * 1) Reading a file containing commit data into a list of strings (one string per commit).
 * 2) When nextTuple() is called, emit a tuple for each string in the list.
 */
@RequiredArgsConstructor
@Component
public class CommitFeedListener extends BaseRichSpout {

    @Autowired
    private MockedQueue queue;

    private SpoutOutputCollector outputCollector;

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