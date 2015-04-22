package tfidf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentWordCountMapper extends Mapper<Text, IntWritable, Text, Text> {

  private static final String SEPARATOR = "|";
  private Text outKey = new Text();
  private Text outValue = new Text();

  public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
    int wordAndDocCounter = value.get();
    String[] wordAndDoc = StringUtils.split(key.toString(), SEPARATOR);
    outKey.set(wordAndDoc[1]);
    outValue.set(wordAndDoc[0] + SEPARATOR + wordAndDocCounter);
    context.write(outKey, outValue);
  }
}