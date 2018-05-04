package stormapplied.githubcommits;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import stormapplied.githubcommits.topology.CommitFeedListener;
import stormapplied.githubcommits.topology.EmailCounter;
import stormapplied.githubcommits.topology.EmailExtractor;

@RequiredArgsConstructor
@SpringBootApplication
public class LocalTopologyRunner implements CommandLineRunner {

    private static final int TEN_MINUTES = 6000;

    @Autowired
    private CommitFeedListener commitFeedListener;

    @Autowired
    private EmailExtractor emailExtractor;

    @Autowired
    private EmailCounter emailCounter;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(LocalTopologyRunner.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", commitFeedListener);

        builder
                .setBolt("email-extractor", emailExtractor)
                .shuffleGrouping("commit-feed-listener");

        builder
                .setBolt("email-counter", emailCounter)
                .fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count-topology",
                config,
                topology);

        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();
    }
}
