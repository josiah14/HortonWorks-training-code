package tfidf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@SuppressWarnings("serial")
class TermWordCountPerDocumentMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, IntWritable> {

  private static Set<String> STOPWORDS;

  static {
    STOPWORDS = new HashSet<String>() {
      {
        add("I");
        add("a");
        add("about");
        add("an");
        add("are");
        add("as");
        add("at");
        add("be");
        add("by");
        add("com");
        add("de");
        add("en");
        add("for");
        add("from");
        add("how");
        add("in");
        add("is");
        add("it");
        add("la");
        add("of");
        add("on");
        add("or");
        add("that");
        add("the");
        add("this");
        add("to");
        add("was");
        add("what");
        add("when");
        add("where");
        add("who");
        add("will");
        add("with");
        add("and");
        add("the");
        add("www");
      }
    };
  }

  private static final IntWritable ONE = new IntWritable(1);
  private static final Pattern WORD_PATTERN = Pattern.compile("\\w+");
  private static final String SEPARATOR = "|";
  private static final String UNDERSCORE = "_";
  private static final Text outKey = new Text();

  public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
      InterruptedException {
    Matcher matcher = WORD_PATTERN.matcher(new String(((ByteBuffer) key.datum().get(SmallFilesWrite.FIELD_CONTENTS))
        .array()));
    String fileName = key.datum().get(SmallFilesWrite.FIELD_FILENAME).toString();

    while (matcher.find()) {
      StringBuilder valueBuilder = new StringBuilder();
      String matchedKey = matcher.group().toLowerCase();
      if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
          || STOPWORDS.contains(matchedKey) || matchedKey.contains(UNDERSCORE)) {
        continue;
      }
      valueBuilder.append(matchedKey);
      valueBuilder.append(SEPARATOR);
      valueBuilder.append(fileName);
      outKey.set(valueBuilder.toString());
      context.write(outKey, ONE);
    }
  }
}
