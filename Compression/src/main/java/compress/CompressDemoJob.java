package compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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


public class CompressDemoJob extends Configured implements Tool {

	public static class CompressMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String searchString;
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split(" ");
			for(String word: words) {
				if(word.contains(searchString)) {
					outputKey.set(words[0] + " " + words[2]);
					context.write(outputKey, value);
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			searchString = context.getConfiguration().get("searchString");
		}
	}


	public static class CompressReducer extends Reducer<Text, Text, NullWritable, Text> {
		private NullWritable outputKey = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			while(values.iterator().hasNext()) {
				context.write(outputKey, values.iterator().next());
			}
		}

	}
	

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "CompressJob");
		Configuration conf = job.getConfiguration();
		conf.set("searchString", args[0]);
		job.setJarByClass(CompressDemoJob.class);
		
		Path out = new Path("logresults");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("logfiles"));
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(CompressMapper.class);
		job.setReducerClass(CompressReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new CompressDemoJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}
}
