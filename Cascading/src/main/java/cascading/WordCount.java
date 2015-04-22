package cascading;

import cascading.flow.*;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.commons.cli.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class WordCount {

  @SuppressWarnings("rawtypes")
  public static Pipe buildWordCountAssembly() {
    Pipe assembly = new Pipe("wordcount");
    Function function = new LineAnalyzer(new Fields("word"));
    assembly = new Each(assembly, new Fields("line"), function);
    assembly = new GroupBy(assembly, new Fields("word"));
    Aggregator count = new Count(new Fields("count"));
    assembly = new Every(assembly, count);
    return assembly;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(new Option("input", true, "Input path for job"));
    options.addOption(new Option("output", true, "Output path for job"));
    options.addOption(new Option("local", false, "Run locally?"));
    options.addOption(new Option("tez", false, "Run with Tez?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    HelpFormatter help = new HelpFormatter();
    if (!cmd.hasOption("input") || !cmd.hasOption("output")) {
      help.printHelp("<cascading jar>", options);
      System.exit(1);
    }

    String inputPath = cmd.getOptionValue("input");
    String outputPath = cmd.getOptionValue("output");
    boolean local = cmd.hasOption("local");
    boolean tez = cmd.hasOption("tez");

    Properties properties = new Properties();

    AppProps.setApplicationJarClass(properties, WordCount.class);
    FlowConnector flowConnector = null;
    Tap source = null;
    Tap sink = null;
    if (local) {
      Scheme sourceScheme = new cascading.scheme.local.TextLine(new Fields(
          "num", "line"));
      source = new FileTap(sourceScheme, inputPath);
      Scheme sinkScheme = new cascading.scheme.local.TextLine(new Fields(
          "word", "count"));
      sink = new FileTap(sinkScheme, outputPath, SinkMode.REPLACE);
      flowConnector = new LocalFlowConnector(properties);
    } else {
      Scheme sourceScheme = new TextLine(new Fields("num", "line"));
      source = new Hfs(sourceScheme, inputPath);
      Scheme sinkScheme = new TextLine(new Fields("word", "count"));
      sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);
      if (tez) {
        properties.put("tez.lib.uris",
            "hdfs:///apps/tez-0.5.0/tez-0.5.0.tar.gz");
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(4)
            .buildProperties(properties);
        flowConnector = new Hadoop2TezFlowConnector(properties);
      } else {
        flowConnector = new Hadoop2MR1FlowConnector(properties);
      }
    }

    FlowDef def = new FlowDef().addSource("wordcount", source)
        .addTailSink(buildWordCountAssembly(), sink).setName("word-count");

    Flow flow = flowConnector.connect(def);
    flow.writeDOT("/tmp/wordcount.dot");
    flow.complete();
  }

  public static class LineAnalyzer extends BaseOperation implements Function {
    public LineAnalyzer(Fields fieldsDeclaration) {
      super(1, fieldsDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      String value = functionCall.getArguments().getString(0);

      if (value == null)
        value = "";


      EnglishAnalyzer analyzer = new EnglishAnalyzer();
      try (TokenStream tStream = analyzer.tokenStream("contents", new StringReader(value))) {
        CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
        tStream.reset();
        while (tStream.incrementToken()) {
          Tuple output = new Tuple();
          output.addString(term.toString());
          functionCall.getOutputCollector().add(output);
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid input value: " + value);
      }
    }
  }
}
