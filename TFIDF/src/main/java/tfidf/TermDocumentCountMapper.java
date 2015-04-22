package tfidf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermDocumentCountMapper extends Mapper<Text, Text, Text, Text> {

  private static final String SEPARATOR = "|";
  private static final String DOC_SEPARATOR = "=";
  
  private Text outKey = new Text();
  private Text outValue = new Text();
  
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    String[] wordAndDoc = StringUtils.split(key.toString(), SEPARATOR);
    outKey.set(wordAndDoc[0]);
    outValue.set(wordAndDoc[1] + DOC_SEPARATOR + value.toString());
    context.write(outKey, outValue);
  }
}