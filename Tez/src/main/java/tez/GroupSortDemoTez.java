package tez;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupSortDemoTez extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(GroupSortDemoTez.class);
  private static final DecimalFormat formatter = new DecimalFormat("###.##%");

  @Override
  public int run(String[] args) throws Exception {    
    Job job = Job.getInstance(getConf(), "group-sort-tez-demo");
    job.setJarByClass(GroupSortDemoTez.class);

    // Initial map phase
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Intermediate reduce phase
    Configuration conf = job.getConfiguration();
    conf.set("mapreduce.framework.name", "yarn-tez");
    conf.setInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 1);
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1, "mapreduce.job.reduce.class"), WordCountReducer.class,
        Reducer.class);
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1, "mapreduce.map.output.key.class"), IntWritable.class,
        Object.class);
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1, "mapreduce.map.output.value.class"), Text.class,
        Object.class);
    conf.setInt(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1, "mapreduce.job.reduces"), 2);

    // Final reduce phase
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);

    TezClient tezClient = new TezClient(new TezConfiguration(conf));

    job.submit();
    JobID jobId = job.getJobID();
    ApplicationId appId = TypeConverter.toYarn(jobId).getAppId();

    DAGClient dagClient = tezClient.getDAGClient(appId);
    DAGStatus dagStatus;
    String[] vNames = { MultiStageMRConfigUtil.getInitialMapVertexName(), MultiStageMRConfigUtil.getIntermediateStageVertexName(1),
        MultiStageMRConfigUtil.getFinalReduceVertexName() };
    while (true) {
      dagStatus = dagClient.getDAGStatus(null);
      if (dagStatus.getState() == DAGStatus.State.RUNNING || dagStatus.getState() == DAGStatus.State.SUCCEEDED
          || dagStatus.getState() == DAGStatus.State.FAILED || dagStatus.getState() == DAGStatus.State.KILLED
          || dagStatus.getState() == DAGStatus.State.ERROR) {
        break;
      }
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
        // continue;
      }
    }

    while (dagStatus.getState() == DAGStatus.State.RUNNING) {
      try {
        printDAGStatus(dagClient, vNames);
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          // continue;
        }
        dagStatus = dagClient.getDAGStatus(null);
      }
      catch (TezException e) {
        LOG.error("Failed to get application progress. Exiting");
        return -1;
      }
    }

    printDAGStatus(dagClient, vNames);
    LOG.info("Application completed, FinalState = {}", dagStatus.getState());
    
    return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames) throws IOException, TezException {
    printDAGStatus(dagClient, vertexNames, false, false);
  }

  public static void printDAGStatus(DAGClient dagClient, String[] vertexNames, boolean displayDAGCounters,
      boolean displayVertexCounters) throws IOException, TezException {
    Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    DAGStatus dagStatus = dagClient.getDAGStatus((displayDAGCounters ? opts : null));
    Progress progress = dagStatus.getDAGProgress();
    double vProgressFloat = 0.0f;
    if (progress != null) {
      System.out.println("");
      System.out.println("DAG: State: "
          + dagStatus.getState()
          + " Progress: "
          + (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) : formatter.format((double) (progress.getSucceededTaskCount())
              / progress.getTotalTaskCount())));
      for (String vertexName : vertexNames) {
        VertexStatus vStatus = dagClient.getVertexStatus(vertexName, (displayVertexCounters ? opts : null));
        if (vStatus == null) {
          System.out.println("Could not retrieve status for vertex: " + vertexName);
          continue;
        }
        Progress vProgress = vStatus.getProgress();
        if (vProgress != null) {
          vProgressFloat = 0.0f;
          if (vProgress.getTotalTaskCount() == 0) {
            vProgressFloat = 1.0f;
          }
          else if (vProgress.getTotalTaskCount() > 0) {
            vProgressFloat = (double) vProgress.getSucceededTaskCount() / vProgress.getTotalTaskCount();
          }
          System.out.println("VertexStatus:" + " VertexName: " + (vertexName.equals("ivertex1") ? "intermediate-reducer" : vertexName)
              + " Progress: " + formatter.format(vProgressFloat));
        }
        if (displayVertexCounters) {
          TezCounters counters = vStatus.getVertexCounters();
          if (counters != null) {
            System.out.println("Vertex Counters for " + vertexName + ": " + counters);
          }
        }
      }
    }
    if (displayDAGCounters) {
      TezCounters counters = dagStatus.getDAGCounters();
      if (counters != null) {
        System.out.println("DAG Counters: " + counters);
      }
    }
  }

  public static void main(String[] args) {
    int result = 0;
    long start = System.currentTimeMillis();
    try {
      result = ToolRunner.run(new Configuration(), new GroupSortDemoTez(), args);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("***TOTAL RUN TIME = {} seconds", (System.currentTimeMillis() - start) / 1000);
    System.exit(result);
  }
}
