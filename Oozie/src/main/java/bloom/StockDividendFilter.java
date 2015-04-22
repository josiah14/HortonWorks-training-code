package bloom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class StockDividendFilter extends Configured implements Tool {
	private static final String FILTER_FILE = "bloom/dividendfilter";

	public static class BloomMapper extends Mapper<LongWritable, Text, IntWritable, BloomFilter> {
		private final IntWritable ONE = new IntWritable(1);
		Stock stock = new Stock();
		private final String COMMA = ",";
		private BloomFilter outputValue;
		private String stockSymbol;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");	
			outputValue = new BloomFilter(10000, 2, Hash.MURMUR_HASH);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split(COMMA);
			String currentSymbol = words[1];
			if(stockSymbol.equals(currentSymbol)) {
				stock.setSymbol(currentSymbol);
				stock.setDate(words[2]);
				outputValue.add(new Key(stock.toString().getBytes()));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(ONE, outputValue);
		}	
	}
	
	public static class BloomReducer extends Reducer<IntWritable, BloomFilter, NullWritable, NullWritable> {
		private BloomFilter allValues;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			allValues = new BloomFilter(10000, 2, Hash.MURMUR_HASH);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<BloomFilter> values, Context context)
				throws IOException, InterruptedException {			
			while(values.iterator().hasNext()) {
				BloomFilter current = values.iterator().next();
				allValues.or(current);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
		    Path path = new Path(FILTER_FILE);
		    FSDataOutputStream out = path.getFileSystem(conf).create(path);
		    allValues.write(out);
		    out.close();
		}
	}
	
	
	public static class StockFilterMapper extends Mapper<LongWritable, Text, Stock, DoubleWritable> {
		private BloomFilter dividends;
		private Stock outputKey = new Stock();
		private DoubleWritable outputValue = new DoubleWritable();
		private String stockSymbol;
		private final String COMMA = ",";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");

			Configuration conf = context.getConfiguration();
		    Path path = new Path(FILTER_FILE);
		    FSDataInputStream in = path.getFileSystem(conf).open(path);
			dividends = new BloomFilter(10000,2,Hash.MURMUR_HASH);
			dividends.readFields(in);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] words = value.toString().split(COMMA);
			String currentSymbol = words[1];
			if(currentSymbol.equals(stockSymbol)) {
				outputKey.setSymbol(currentSymbol);
				outputKey.setDate(words[2]);
				Key stockKey = new Key(outputKey.toString().getBytes());
				if(dividends.membershipTest(stockKey)) {
					outputValue.set(Double.parseDouble(words[6]));
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class StockFilterReducer extends Reducer<Stock, DoubleWritable, Text, DoubleWritable> {
		private String stockSymbol = "";
		private Text outputKey = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");
		}

		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			//Check for a false positive
			if(!stockSymbol.equals(key.getSymbol())) {
				System.out.println("False positive: " + key.getSymbol());
			} else {
				while(values.iterator().hasNext()) {
					DoubleWritable closingPrice = values.iterator().next();
					outputKey.set(key.toString());
					context.write(outputKey, closingPrice);
				}
			}
		}

	}


	@Override
	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "CreateBloomFilter");
		job1.setJarByClass(getClass());
		Configuration conf = job1.getConfiguration();
		conf.set("stockSymbol", args[0]);

		FileInputFormat.setInputPaths(job1, new Path("dividends"));
		
		job1.setMapperClass(BloomMapper.class);
		job1.setReducerClass(BloomReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(NullOutputFormat.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(BloomFilter.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setNumReduceTasks(1);
		
		boolean job1success = job1.waitForCompletion(true);
		if(!job1success) {
			System.out.println("The CreateBloomFilter job failed!");
			return -1;
		}

		Job job2 = Job.getInstance(conf, "FilterStocksJob");
		job2.setJarByClass(getClass());
		conf = job2.getConfiguration();

		Path out = new Path("bloomoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job2, new Path("stocks"));
		FileOutputFormat.setOutputPath(job2, out);
		
		job2.setMapperClass(StockFilterMapper.class);
		job2.setReducerClass(StockFilterReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(Stock.class);
		job2.setMapOutputValueClass(DoubleWritable.class);	
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
				
		boolean job2success = job2.waitForCompletion(true);
		if(!job2success) {
			System.out.println("The FilterStocksJob failed!");
			return -1;
		}		
		return 1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new StockDividendFilter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
