package cascading;

import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.Identity;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.FirstBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import com.google.common.collect.Ordering;

public class StockAnalyzer {

  public static Pipe buildStockAnalysisAssembly() {
    DateParser dateParser = new DateParser(new Fields("date"), "yyyy-MM-dd");
    DateFormatter dateFormatter = new DateFormatter(new Fields("date"), "yyyy");
    Pipe lhs = new Discard(new Pipe("stocks"), new Fields("exchange",
        "stock_price_close", "stock_volume", "stock_price_adj_close"));

    Pipe rhs = new Discard(new Pipe("dividends"), new Fields("exchange"));
    rhs = new Rename(rhs, new Fields("stock_symbol", "date"), new Fields(
        "div_symbol", "div_date"));

    Pipe assembly = new CoGroup(lhs, new Fields("stock_symbol", "date"), rhs,
        new Fields("div_symbol", "div_date"), new LeftJoin());

    assembly = new Each(assembly, new Fields("stock_symbol", "date",
        "stock_price_high", "stock_price_low", "dividends"), new Identity());
    assembly = new Coerce(assembly, new Fields("stock_price_high",
        "stock_price_low", "dividends"), double.class, double.class,
        double.class);

    assembly = new Each(assembly, new Fields("date"), dateParser,
        Fields.REPLACE);
    assembly = new Each(assembly, new Fields("date"), dateFormatter,
        Fields.REPLACE);

    Fields groupingFields = new Fields("stock_symbol", "date");

    Fields high = new Fields("stock_price_high");
    high.setComparators(Ordering.natural().reverse());
    FirstBy maxHigh = new FirstBy(high, new Fields("max_high"));

    Fields low = new Fields("stock_price_low");
    low.setComparators(Ordering.natural());
    FirstBy minLow = new FirstBy(low, new Fields("min_low"));

    Fields dividends = new Fields("dividends");
    dividends.setComparators(Ordering.natural().reverse());
    FirstBy maxDividend = new FirstBy(dividends, new Fields("max_dividend"));

    assembly = new AggregateBy(assembly, groupingFields, maxHigh, minLow,
        maxDividend);

    return assembly;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(new Option("stocks", true, "Stocks input path for job"));
    options.addOption(new Option("dividends", true,
        "Dividends input path for job"));
    options.addOption(new Option("output", true, "Output path for job"));
    options.addOption(new Option("local", false, "Run locally?"));
    options.addOption(new Option("tez", false, "Run with Tez?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    HelpFormatter help = new HelpFormatter();
    if (!cmd.hasOption("stocks") || !cmd.hasOption("dividends")
        || !cmd.hasOption("output")) {
      help.printHelp("<cascading jar>", options);
      System.exit(1);
    }

    String stocksPath = cmd.getOptionValue("stocks");
    String dividendsPath = cmd.getOptionValue("dividends");
    String outputPath = cmd.getOptionValue("output");
    boolean local = cmd.hasOption("local");
    boolean tez = cmd.hasOption("tez");

    Properties properties = new Properties();
    AppProps.setApplicationJarClass(properties, StockAnalyzer.class);
    FlowConnector flowConnector = null;
    Tap stocksSource = null;
    Tap dividendsSource = null;
    Tap sink = null;

    if (local) {
      Scheme stockSourceScheme = new cascading.scheme.local.TextDelimited(true,
          ",");
      Scheme dividendSourceScheme = new cascading.scheme.local.TextDelimited(
          true, ",");
      stocksSource = new FileTap(stockSourceScheme, stocksPath);
      dividendsSource = new FileTap(dividendSourceScheme, dividendsPath);
      Scheme sinkScheme = new cascading.scheme.local.TextDelimited(true, ",");
      sink = new FileTap(sinkScheme, outputPath, SinkMode.REPLACE);
      flowConnector = new LocalFlowConnector(properties);
    }
    else {
      Scheme stockSourceScheme = new TextDelimited(true, ",");
      Scheme dividendSourceScheme = new TextDelimited(true, ",");
      stocksSource = new Hfs(stockSourceScheme, stocksPath);
      dividendsSource = new Hfs(dividendSourceScheme, dividendsPath);
      Scheme sinkScheme = new TextDelimited(false, ",");
      sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);
      if (tez) {
        properties.put("tez.lib.uris",
            "hdfs:///apps/tez-0.5.0/tez-0.5.0.tar.gz");
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(4)
            .buildProperties(properties);
        flowConnector = new Hadoop2TezFlowConnector(properties);
      }
      else {
        flowConnector = new Hadoop2MR1FlowConnector(properties);
      }
    }

    FlowDef def = new FlowDef().addSource("stocks", stocksSource)
        .addSource("dividends", dividendsSource)
        .addTailSink(buildStockAnalysisAssembly(), sink)
        .setName("stock-analyzer");

    Flow flow = flowConnector.connect(def);
    flow.complete();
  }
}
