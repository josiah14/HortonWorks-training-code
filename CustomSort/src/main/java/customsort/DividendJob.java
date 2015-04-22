package customsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DividendJob extends Configured implements Tool {

	
	public static class DividendGrowthMapper extends Mapper<LongWritable, Text, Stock, DoubleWritable> {
		private Stock outputKey = new Stock();
		private DoubleWritable outputValue = new DoubleWritable();
		private final String EXCHANGE = "exchange";
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString(),'\\',',');
			if(EXCHANGE.equals(words[0])) {
				return;
			}
			
			outputKey.setSymbol(words[1]);
			outputKey.setDate(words[2]);
			outputValue.set(Double.parseDouble(words[3]));
			context.write(outputKey, outputValue);
		}
	}

	public static class StockPartitioner extends Partitioner<Stock, DoubleWritable> {

		@Override
		public int getPartition(Stock key, DoubleWritable value, int numReduceTasks) {
			return 0;
		}		
	}

	
	public static class DividendGrowthReducer extends Reducer<Stock, DoubleWritable, NullWritable, DividendChange> {
		private NullWritable outputKey = NullWritable.get();
		private DividendChange outputValue = new DividendChange();
		
		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double previousDividend = 0.0;
			for(DoubleWritable dividend : values) {
				double currentDividend = dividend.get();
				double growth = currentDividend - previousDividend;
				if(Math.abs(growth) > 0.000001) {
					outputValue.setSymbol(key.getSymbol());
					outputValue.setDate(key.getDate());
					outputValue.setChange(growth);
					context.write(outputKey, outputValue);
					previousDividend = currentDividend;
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "DividendJob");
		job.setJarByClass(DividendJob.class);
		
		Path out = new Path("growth");
		FileInputFormat.setInputPaths(job, new Path("dividends"));
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);
		
		job.setMapperClass(DividendGrowthMapper.class);
		job.setReducerClass(DividendGrowthReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DividendChange.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(DoubleWritable.class);
				
		job.setNumReduceTasks(3);

		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new DividendJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
