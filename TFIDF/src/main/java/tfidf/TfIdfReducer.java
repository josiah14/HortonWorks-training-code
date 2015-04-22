package tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TfIdfReducer extends Reducer<Text, Text, Text, Text> {

  private static final DecimalFormat DF = new DecimalFormat("###.########");
  private static final String DOC_SEPARATOR = "=";
  private static final String SEPARATOR = "|";
  private Text outKey = new Text();
  private Text outValue = new Text();

  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long totalDocs = context.getConfiguration().getLong("totalDocs", 0);

    int totalDocsForWord = 0;
    Map<String, String> tempFrequencies = new HashMap<String, String>();
    for (Text value : values) {
      String[] documentAndFrequencies = StringUtils.split(value.toString(), DOC_SEPARATOR);
      totalDocsForWord++;
      tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
    }
    for (String document : tempFrequencies.keySet()) {
      String[] wordFrequencyAndTotalWords = StringUtils.split(tempFrequencies.get(document), SEPARATOR);

      // TF
      double tf = Double.valueOf(wordFrequencyAndTotalWords[0]) / Double.valueOf(wordFrequencyAndTotalWords[1]);

      // IDF
      double idf = (double) totalDocs / totalDocsForWord;

      double tfIdf = tf * Math.log10(idf);

      outKey.set(key + SEPARATOR + document);
      outValue.set(DF.format(tfIdf));
      context.write(outKey, outValue);
    }
  }
}