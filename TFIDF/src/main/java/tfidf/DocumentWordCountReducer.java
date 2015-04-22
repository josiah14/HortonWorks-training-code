package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentWordCountReducer extends Reducer<Text, Text, Text, Text> {
  private static final String SEPARATOR = "|";
  private Text outKey = new Text();
  private Text outValue = new Text();

  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int sumOfWordsInDocument = 0;
    Map<String, Integer> tempCounter = new HashMap<String, Integer>();
    for (Text val : values) {
      String[] wordCounter = StringUtils.split(val.toString(), SEPARATOR);
      tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
      sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
    }
    for (String wordKey : tempCounter.keySet()) {
      outKey.set(wordKey + SEPARATOR + key.toString());
      outValue.set(tempCounter.get(wordKey) + SEPARATOR + sumOfWordsInDocument);
      context.write(outKey, outValue);
    }
  }
}
