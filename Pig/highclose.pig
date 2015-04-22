register stockudfs.jar;

define HighestClosingPriceWindow stockudfs.HighestClosingPriceWindow('4');

stockdata = LOAD 'stocksA' using PigStorage(',') AS (
	exchange:chararray, 
	symbol:chararray, 
	date:chararray, 
	open:float, 
	high:float, 
	low:float, 
	close:float, 
	volume:int
);
stocks_all = FOREACH stockdata GENERATE symbol, date, close;
stocks_group = GROUP stocks_all BY symbol;
stocks_high = FOREACH stocks_group {
	sorted = ORDER stocks_all BY date ASC;
	GENERATE group as symbol, HighestClosingPriceWindow(sorted) as result; 
}
dump stocks_high;
