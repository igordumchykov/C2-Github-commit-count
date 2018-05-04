package stormapplied.githubcommits;


import lombok.RequiredArgsConstructor;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
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

    private static final int SLEEP_TIME = 3000;

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

        Utils.sleep(SLEEP_TIME);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();
    }
}
