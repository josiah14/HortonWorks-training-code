package average;

import java.io.IOException;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageJob extends Configured implements Tool {
	public enum Counters {MAP, COMBINE, REDUCE};
    private static final int STEP = 1;

	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text state = new Text();
		private Text medianIncomeY2k = new Text();
		private static final String ONE = ",1";

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] values = StringUtils.split(value.toString(), '\\', ',');
			state.set(values[1].trim());
			medianIncomeY2k.set(values[9].concat(ONE));
			context.write(state, medianIncomeY2k);
			context.getCounter(Counters.MAP).increment(STEP);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("Map counter = " + context.getCounter(Counters.MAP).getValue());
		}
	}

	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();
		private static final String COMMA = ",";

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			int count = 0;
			
			for (Text value : values) {
				String [] pair = StringUtils.split(value.toString(), '\\', ',');
				sum += Long.parseLong(pair[0]);
				count += Integer.parseInt(pair[1]);
			}
			outputValue.set(sum + COMMA + count);
			context.write(key, outputValue);
			context.getCounter(Counters.COMBINE).increment(STEP);
		}		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("Combine counter = " + context.getCounter(Counters.COMBINE).getValue());
		}
	}

	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable average = new DoubleWritable();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Text value : values) {
				String [] pair = StringUtils.split(value.toString(), '\\', ',');
				sum += Double.parseDouble(pair[0]);
				count += Integer.parseInt(pair[1]);
			}
			average.set(sum / count);
			context.write(key, average);
			context.getCounter(Counters.REDUCE).increment(STEP);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("Reduce counter = " + context.getCounter(Counters.REDUCE).getValue());
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "AverageJob");
		job.setJarByClass(AverageJob.class);

		Path out = new Path("output");
		FileInputFormat.setInputPaths(job, "counties");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);

		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageCombiner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


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
