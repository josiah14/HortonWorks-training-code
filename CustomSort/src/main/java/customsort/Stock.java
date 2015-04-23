package customsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Stock implements WritableComparable<Stock> {
	private String symbol;
	private String date;
	
	public String getSymbol() {
		return symbol;
	}
	
	public void setSymbol(String sym) {
		symbol = sym;
	}
	
	public String getDate() {
		return date;
	}
	
	public void setDate(String dt) {
		date = dt;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(symbol);
		out.writeUTF(date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = in.readUTF();
		date = in.readUTF();
	}

	@Override
	public int compareTo(Stock o) {
		int response = symbol.compareTo(o.getSymbol());
		return response == 0 ? date.compareTo(o.getDate()) : response;
	}
}
