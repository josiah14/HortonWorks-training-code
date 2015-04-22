package tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupSortDemo extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(GroupSortDemo.class);

  @Override
  public int run(String[] args) throws Exception {    
    Job job1 = Job.getInstance(getConf(), "group-mapreduce-demo-job-1");
    job1.setJarByClass(GroupSortDemo.class);

    job1.setMapperClass(WordCountMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    job1.setReducerClass(WordCountReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    job1.setNumReduceTasks(2);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    Path outputPath1 = new Path(args[1] + "-intermediate");
    outputPath1.getFileSystem(job1.getConfiguration()).delete(outputPath1, true);
    FileOutputFormat.setOutputPath(job1, outputPath1);

    boolean result = job1.waitForCompletion(true);

    if (!result) {
      LOG.error("Failed to complete grouping MR job");
      return -1;
    }

    Job job2 = Job.getInstance(getConf(), "sort-mapreduce-demo-job-2");
    job2.setJarByClass(GroupSortDemo.class);
    
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(Mapper.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);
    
    job2.setReducerClass(Reducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job2, outputPath1);
    Path outputPath2 = new Path(args[1]);
    outputPath2.getFileSystem(job2.getConfiguration()).delete(outputPath2, true);
    FileOutputFormat.setOutputPath(job2, outputPath2);
    
    result = job2.waitForCompletion(true);
    return result ? 0 : -1;
  }

  public static void main(String[] args) {
    int result = 0;
    long start = System.currentTimeMillis();
    try {
      result = ToolRunner.run(new Configuration(), new GroupSortDemo(), args);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("***TOTAL RUN TIME = {} seconds", (System.currentTimeMillis() - start) / 1000);
    System.exit(result);
  }
}
