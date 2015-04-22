package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovingAveragePreprocessor extends Configured implements Tool {

	public static class PreprocessorMapper extends Mapper<Stock, StockPrices, Stock, DoubleWritable> {
		private DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void map(Stock key, StockPrices value, Context context)
				throws IOException, InterruptedException {
			outputValue.set(value.getClose());
			context.write(key, outputValue);
		}
		
		
	}
	
	

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "MovingAveragePreprocessor");
		job.setJarByClass(MovingAveragePreprocessor.class);

		Path out = new Path("closingprices");
		FileInputFormat.setInputPaths(job, "stocks");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);

		job.setMapperClass(PreprocessorMapper.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(StockInputFormat.class);		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Stock.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), new MovingAveragePreprocessor(), args);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
