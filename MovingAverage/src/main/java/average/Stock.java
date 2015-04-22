package average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Stock implements WritableComparable<Stock> {
	private String symbol;
	private String date;

	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = in.readUTF();
		date = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(symbol);
		out.writeUTF(date);
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public int compareTo(Stock arg0) {
		int response = symbol.compareTo(arg0.symbol);
		if(response == 0) {
			response = date.compareTo(arg0.date);
		}
		return response;
	}

	public String toString() {
		return symbol + "\t" + date;
	}
}
