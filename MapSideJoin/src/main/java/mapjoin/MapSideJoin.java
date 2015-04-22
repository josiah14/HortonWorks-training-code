package mapjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {

	public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Stock, StockPrices> {
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

		}
	}


	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MapSideJoinJob");
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		

		Path out = new Path("joinoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("stocks"));
		FileOutputFormat.setOutputPath(job, out);
		
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(StockPrices.class);
		
		job.setNumReduceTasks(0);
		

		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new MapSideJoin(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
