package storm;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.spout.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class HdfsPersistence {

  public static void main(String[] args) throws Exception {

    Options options = new Options();
    options.addOption(new Option("local", false, "Run locally?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    boolean local = cmd.hasOption("local");

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    FileSystem.get(new Configuration()).delete(new Path("/user/root/storm-hdfs"), true);
    
    SyncPolicy syncPolicy = new CountSyncPolicy(10);
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.KB);
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/root/storm-hdfs/");
    HdfsBolt bolt = new HdfsBolt().withFsUrl("hdfs://namenode:8020").withFileNameFormat(fileNameFormat)
        .withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy).withRecordFormat(new DelimitedRecordFormat());

    builder.setBolt("hdfs", bolt, 8).shuffleGrouping("spout");

    Config conf = new Config();
    conf.setDebug(true);

    if (local) {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("hdfs", conf, builder.createTopology());
      Thread.sleep(20000);
      cluster.shutdown();
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology("hdfs", conf, builder.createTopology());
    }
  }
}
