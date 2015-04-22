package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageJob extends Configured implements Tool {
	
	public static class AveragePartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			char firstLetter = key.toString().trim().charAt(0);
			return firstLetter % numPartitions;
		}
	}

	public enum Counters {MAP, COMBINE, REDUCE} 

	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] values = value.toString().split(",");
			outputKey.set(values[1].trim());
			outputValue.set(values[9].trim() + ",1");
			context.write(outputKey, outputValue);
			context.getCounter(Counters.MAP).increment(1);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("MAP counter = " + context.getCounter(Counters.MAP).getValue());
		}
	}

	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			int count = 0;
			while(values.iterator().hasNext()) {
				String [] current = values.iterator().next().toString().split(",");
				sum += Integer.parseInt(current[0]);
				count += Integer.parseInt(current[1]);
			}
			outputValue.set(sum + "," + count);
			context.write(key, outputValue);
			context.getCounter(Counters.COMBINE).increment(1);
		}		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("COMBINE counter = " + context.getCounter(Counters.COMBINE).getValue());
		}
	}

	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			while(values.iterator().hasNext()) {
				String [] current = values.iterator().next().toString().split(",");
				sum += Long.parseLong(current[0]);
				count += Integer.parseInt(current[1]);
			}
			outputValue.set(sum/count);
			context.write(key, outputValue);
			context.getCounter(Counters.REDUCE).increment(1);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println(context.getCounter(Counters.REDUCE).getValue());
		}
	}	

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "AverageJob");
		job.setJarByClass(AverageJob.class);

		Path out = new Path("counties/output");
		out.getFileSystem(conf).delete(out, true);
		FileInputFormat.setInputPaths(job, "counties");
		FileOutputFormat.setOutputPath(job, out);
		

		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageCombiner.class);
		job.setPartitionerClass(AveragePartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new AverageJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);
	}
}
