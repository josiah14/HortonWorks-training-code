package partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Population extends Configured implements Tool {


	public static class PopulationMapper extends Mapper<Text, Text, Text, Text> {

		private Text outputValue = new Text();

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split("\t");
			String population = words[3];
			outputValue.set(population);
			context.write(key, outputValue);
		}
	}

	public static class PopulationReducer extends Reducer<Text, Text, Text, LongWritable> {
		private LongWritable population = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			while(values.iterator().hasNext()) {
				long currentValue = Long.parseLong(values.iterator().next().toString());
				sum += currentValue;
			}
			population.set(sum);
			context.write(key, population);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "PopulationJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Population.class);

		Path out = new Path("totalorder");
		FileInputFormat.setInputPaths(job, "populations");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);

		job.setMapperClass(PopulationMapper.class);
		job.setReducerClass(PopulationReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(5);

		//Configure the TotalOrderPartitioner here...



		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new Population(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
