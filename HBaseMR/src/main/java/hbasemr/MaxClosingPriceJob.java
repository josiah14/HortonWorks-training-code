package hbasemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class MaxClosingPriceJob extends Configured implements Tool {

	public static class MaxClosingPriceMapper extends TableMapper<Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
	}
	
	public static class MaxClosingPriceReducer extends TableReducer<Text, Text, Text> {

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());
		Job job = Job.getInstance(conf, "MaxClosingPriceJob");
		job.setJarByClass(MaxClosingPriceJob.class);
		TableMapReduceUtil.addDependencyJars(job);

		
		
		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new MaxClosingPriceJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}
}
