package stormapplied.githubcommits.topology;


import lombok.RequiredArgsConstructor;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
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