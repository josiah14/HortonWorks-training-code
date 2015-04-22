package bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Stock implements WritableComparable<Stock> {

	private String symbol;
	private String date;
	private static final String COMMA = ",";
	
	public Stock() {}
	
	public Stock(String symbol, String date) {
		this.symbol = symbol;
		this.date = date;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Stock) {
			Stock other = (Stock) obj;
			if(symbol.equals(other.symbol) && date.equals(other.date)) {
				return true;
			}
		} 
		return false;
	}

	@Override
	public int hashCode() {
		return (symbol + date).hashCode();
	}

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

	@Override
	public int compareTo(Stock arg0) {
		int response = this.symbol.compareTo(arg0.symbol);
		if(response == 0) {
			response = this.date.compareTo(arg0.date);
		}
		return response;
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
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(symbol).append(COMMA).append(date);
		return sb.toString();
	}
}
