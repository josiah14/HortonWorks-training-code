package scalding

import java.text.SimpleDateFormat
import java.util.TimeZone
import com.twitter.scalding._
import cascading.pipe.joiner.LeftJoin
import cascading.property.AppProps

class StockAnalyzer(args: Args) extends Job(args) {
  implicit val dp: DateParser = DateParser.from(new SimpleDateFormat("yyyy-MM-dd"))
  implicit val tz: TimeZone = TimeZone.getDefault()

  val stocks = Csv(args("input"), quote = "", skipHeader = true, writeHeader = true)
    .discard('exchange, 'stock_price_close, 'stock_volume, 'stock_price_adj_close)

  val dividends = Csv(args("dividends"), quote = "", skipHeader = true, writeHeader = true)
    .discard('exchange)
    .rename(('stock_symbol, 'date) -> ('div_symbol, 'div_date))

  val joined = stocks.joinWithSmaller(('stock_symbol, 'date) -> ('div_symbol, 'div_date), dividends, new LeftJoin())
    .discard('div_symbol, 'div_date)
    .map('date -> 'date) { s: String => dp.parse(s).getOrElse("Invalid Date") }
    .map(('stock_price_high, 'stock_price_low, 'dividends) -> ('stock_price_high, 'stock_price_low, 'dividends)) {
      x: (Double, Double, Double) => x
    }
    .groupBy('stock_symbol) {
      _.max('stock_price_high).min('stock_price_low).max('dividends)

    }
    .write(Tsv(args("output"), writeHeader = true))

  override def config: Map[AnyRef, AnyRef] = {
    val config = super.config
    if (args.boolean("tez")) {
      config ++ Map("cascading.flow.runtime.gather.partitions.num" -> "4",
        "tez.lib.uris" -> "hdfs:///apps/tez-0.5.0/tez-0.5.0.tar.gz",
        "cascading.app.appjar.class" -> this.getClass())
    } else {
      config ++ Map("cascading.app.appjar.class" -> this.getClass())
    }
  }

  override def run: Boolean = {
    val flow = buildFlow
    flow.complete

    if (!args.boolean("tez")) {
      val statsData = flow.getFlowStats
      handleStats(statsData)
      statsData.isSuccessful
    } 
    else {
      true
    }
  }
}