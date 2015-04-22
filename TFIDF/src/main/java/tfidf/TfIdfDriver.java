package tfidf;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIdfDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job1 = Job.getInstance(getConf(), "TermWordCountPerDocument");
    job1.setJarByClass(getClass());
    Configuration conf1 = job1.getConfiguration();
    FileInputFormat.setInputPaths(job1, new Path("enron/mann.avro"));
    Path out1 = new Path("tfidf/step1");
    out1.getFileSystem(conf1).delete(out1, true);
    FileOutputFormat.setOutputPath(job1, out1);
    FileOutputFormat.setOutputCompressorClass(job1, SnappyCodec.class);
    FileOutputFormat.setCompressOutput(job1, true);
    
    job1.setMapperClass(TermWordCountPerDocumentMapper.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setInputFormatClass(AvroKeyInputFormat.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    Job job2 = Job.getInstance(getConf(), "DocumentWordCount");
    job2.setJarByClass(getClass());
    Configuration conf2 = job2.getConfiguration();
    FileInputFormat.setInputPaths(job2, new Path("tfidf/step1"));
    Path out2 = new Path("tfidf/step2");
    out2.getFileSystem(conf2).delete(out2, true);
    FileOutputFormat.setOutputPath(job2, out2);
    FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);
    FileOutputFormat.setCompressOutput(job2, true);

    job2.setMapperClass(DocumentWordCountMapper.class);
    job2.setReducerClass(DocumentWordCountReducer.class);
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    Job job3 = Job.getInstance(getConf(), "DocumentCountAndTfIdf");
    job3.setJarByClass(getClass());
    Configuration conf3 = job3.getConfiguration();
    FileInputFormat.setInputPaths(job3, new Path("tfidf/step2"));
    Path out3 = new Path("tfidf/final");
    out3.getFileSystem(conf3).delete(out3, true);
    FileOutputFormat.setOutputPath(job3, out3);
    FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);
    FileOutputFormat.setCompressOutput(job3, true);    
    
    //Get the total document count from the Avro file metadata
    DataFileReader<Object> reader =
        new DataFileReader<Object>(new FsInput(new Path("enron/mann.avro"), conf3),
            new GenericDatumReader<Object>()); 
    conf3.setLong("totalDocs",
        reader.getMetaLong("recordCount"));
    reader.close();

    job3.setMapperClass(TermDocumentCountMapper.class);
    job3.setReducerClass(TfIdfReducer.class);
    job3.setInputFormatClass(SequenceFileInputFormat.class);
    job3.setOutputFormatClass(SequenceFileOutputFormat.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);



    return 0;

  }

  public static void main(String[] args) {
    int result = 0;
    try {
      result = ToolRunner.run(new Configuration(), new TfIdfDriver(), args);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(result);

  }

}
