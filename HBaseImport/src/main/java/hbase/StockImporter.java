package hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StockImporter extends Configured implements Tool {

	public static class StockImporterMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
		private final String COMMA = ",";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split(COMMA);
			if(words[0].equals("exchange")) return;
			
			String symbol = words[1];
			String date = words[2];
			double highPrice = Double.parseDouble(words[4]);
			double lowPrice = Double.parseDouble(words[5]);
			double closingPrice = Double.parseDouble(words[6]);
			double volume = Double.parseDouble(words[7]);

		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}
	}


	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "StockImportJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(StockImporter.class);
		
		FileInputFormat.setInputPaths(job, new Path("stocksA"));
		
		job.setMapperClass(StockImporterMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new StockImporter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}
}
