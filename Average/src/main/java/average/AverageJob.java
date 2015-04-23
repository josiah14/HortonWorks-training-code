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
	public enum Counters {MAP, COMBINE, REDUCE} 
	private static final int ONE = 1;
	private static final String COMMA = ",";

	
	public static class AveragePartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text state, Text partialSum, int numPartitions) {
			return state.toString().trim().charAt(0) % numPartitions;
		}
	}

	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text state = new Text();
		private Text medianIncomeY2k = new Text();
        private static final String COMMA_ONE = ",1";
		
		@Override
		protected void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			String [] pairs = line.toString().split(COMMA);
			state.set(pairs[1].trim());
			medianIncomeY2k.set(pairs[9].trim() + COMMA_ONE);
			context.write(state, medianIncomeY2k);
			context.getCounter(Counters.MAP).increment(ONE);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("MAP counter = " + context.getCounter(Counters.MAP).getValue());
		}
	}

	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text partialSum = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			int count = 0;
			while(values.iterator().hasNext()) {
				String [] pair = values.iterator().next().toString().split(COMMA);
				sum += Integer.parseInt(pair[0]);
				count += Integer.parseInt(pair[1]);
			}
			partialSum.set(sum + COMMA + count);
			context.write(key, partialSum);
			context.getCounter(Counters.COMBINE).increment(ONE);
		}		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("COMBINE counter = " + context.getCounter(Counters.COMBINE).getValue());
		}
	}

	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		DoubleWritable average = new DoubleWritable();
		
		@Override
		protected void reduce(Text state, Iterable<Text> partialSums, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			while(partialSums.iterator().hasNext()) {
				String [] pair = partialSums.iterator().next().toString().split(COMMA);
				sum += Long.parseLong(pair[0]);
				count += Integer.parseInt(pair[1]);
			}
			average.set(sum/count);
			context.write(state, average);
			context.getCounter(Counters.REDUCE).increment(ONE);
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

		return job.waitForCompletion(true) ? 0 : 1;
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
