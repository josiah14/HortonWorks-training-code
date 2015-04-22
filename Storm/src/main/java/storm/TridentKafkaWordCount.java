package storm;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;

public class TridentKafkaWordCount {
  public static class Split extends BaseFunction {
    private static final long serialVersionUID = -8451631476210125187L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
    TridentKafkaConfig spoutConfig = new TridentKafkaConfig(new ZkHosts("namenode:2181,resourcemanager:2181,hiveserver:2181"), "sentences");
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.forceFromStart = true;

    TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConfig);

    TridentTopology topology = new TridentTopology();
    TridentState wordCounts = topology.newStream("spout", kafkaSpout).parallelismHint(16)
        .each(new Fields("str"), new Split(), new Fields("word"))
        .groupBy(new Fields("word"))
        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        .parallelismHint(16);

    topology.newDRPCStream("words", drpc)
        .each(new Fields("args"), new Split(), new Fields("word"))
        .groupBy(new Fields("word"))
        .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
        .each(new Fields("count"), new FilterNull())
        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option("local", false, "Run locally?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    boolean local = cmd.hasOption("local");

    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (local) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("trident-kafka-word-count", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT: " + drpc.execute("words", "good happy"));
        Thread.sleep(1000);
      }
      drpc.shutdown();
      cluster.shutdown();
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology("trident-kafka-word-count", conf, buildTopology(null));
      DRPCClient client = new DRPCClient("namenode", 3772);
      for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT: " + client.execute("words", "good happy"));
        Thread.sleep(1000);
      }
      client.close();
    }
  }
}
